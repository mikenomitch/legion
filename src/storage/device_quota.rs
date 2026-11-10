//! Namespace quota for devices (`quota.<band>.devices`, docs/devices.md,
//! "Quota"; docs/namespaces.md).
//!
//! A job is charged per band the larger of what it declares (each
//! count-driven task's effective count times its device requests) and what
//! it holds (the devices assigned to its live allocations) — the rule cpu
//! and memory follow (`crate::storage::namespace_policy_store`). Counts are
//! kept by name: declared under the request's name (`gpu`, `nvidia/gpu`),
//! held under the assigned group (`nvidia/gpu/A100-SXM4-80GB`). A quota key
//! is charged every count whose name is [`DeviceName::compatible`] with it,
//! so a request cannot slip past an `nvidia/gpu` cap by asking for `gpu`.

use std::collections::BTreeMap;

use rusqlite::Connection;

use crate::devices::DeviceName;
use crate::prelude::*;
use crate::storage::errors::StorageError;
use crate::storage::namespace_policy_store::{
    count_driven, effective_count, scale_overrides_on, ScaleOverrides,
};
use crate::structs::namespace::job_namespace;
use crate::structs::namespace_policy::NamespacePolicy;
use crate::structs::JobConfig;

/// Device counts by name.
pub type DeviceCounts = BTreeMap<String, f64>;

/// One job's device charge, by band.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JobDeviceCharge {
    pub declared: BTreeMap<String, DeviceCounts>,
    pub live: BTreeMap<String, DeviceCounts>,
}

impl JobDeviceCharge {
    /// What the job is charged against `key` in `band`.
    pub fn charged(&self, band: &str, key: &DeviceName) -> f64 {
        let sum = |counts: Option<&DeviceCounts>| -> f64 {
            counts
                .map(|counts| {
                    counts
                        .iter()
                        .filter(|(name, _)| {
                            DeviceName::parse(name).is_ok_and(|name| name.compatible(key))
                        })
                        .map(|(_, count)| *count)
                        .sum()
                })
                .unwrap_or(0.0)
        };
        sum(self.declared.get(band)).max(sum(self.live.get(band)))
    }
}

/// The device counts a task's resources ask for, by request name.
fn task_devices(resources: &serde_json::Value) -> DeviceCounts {
    let mut counts = DeviceCounts::new();
    for spec in crate::devices::request_specs(resources).unwrap_or_default() {
        *counts.entry(spec.name).or_default() += f64::from(spec.count);
    }
    counts
}

fn add_counts(into: &mut DeviceCounts, from: &DeviceCounts, times: f64) {
    for (name, count) in from {
        *into.entry(name.clone()).or_default() += count * times;
    }
}

/// The declared half for one config about to be written.
fn declared_for_config(
    config: &JobConfig,
    overrides: &ScaleOverrides,
) -> BTreeMap<String, DeviceCounts> {
    let band = config.priority.as_str().to_string();
    let mut out: BTreeMap<String, DeviceCounts> = BTreeMap::new();
    for task in &config.tasks {
        if !count_driven(task.count.per.as_deref(), task.stateful.as_ref()) {
            continue;
        }
        let devices = task_devices(&task.resources);
        if devices.is_empty() {
            continue;
        }
        let count = effective_count(
            overrides,
            &config.name,
            &task.group,
            &task.name,
            task.count.num,
        );
        add_counts(
            out.entry(band.clone()).or_default(),
            &devices,
            f64::from(count),
        );
    }
    out
}

/// Every job's device charge, keyed by canonical job id. Only jobs that
/// declare or hold devices appear.
pub fn job_device_charges_on(conn: &Connection) -> R<BTreeMap<String, JobDeviceCharge>> {
    let overrides = scale_overrides_on(conn)?;
    let mut charges: BTreeMap<String, JobDeviceCharge> = BTreeMap::new();
    let mut declared = conn.prepare_cached(
        "SELECT j.id, j.status, jc.priority, tc.group_name, tc.name, tc.count, tc.count_per,
                tc.stateful, tc.resources
         FROM job j
         JOIN job_config jc ON jc.id = (
             SELECT latest.id FROM job_config latest WHERE latest.job_id = j.id
             ORDER BY latest.version DESC LIMIT 1)
         JOIN task_config tc ON tc.job_config_id = jc.id
         WHERE json_valid(tc.resources) AND json_type(tc.resources, '$.devices') = 'array';",
    )?;
    let rows = declared.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, u32>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, serde_json::Value>(8)?,
        ))
    })?;
    for row in rows {
        let (job, status, band, group, task, count, per, stateful, resources) = row?;
        let stateful: Option<crate::structs::StatefulConfig> = stateful
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok());
        if status == "stopped" || !count_driven(per.as_deref(), stateful.as_ref()) {
            continue;
        }
        let devices = task_devices(&resources);
        if devices.is_empty() {
            continue;
        }
        let count = effective_count(&overrides, &job, &group, &task, count);
        let band = crate::structs::Band::from_stored(&band)
            .as_str()
            .to_string();
        add_counts(
            charges
                .entry(job)
                .or_default()
                .declared
                .entry(band)
                .or_default(),
            &devices,
            f64::from(count),
        );
    }
    let mut live = conn.prepare_cached(&format!(
        "SELECT tc.job_id, jc.priority, a.resources
         FROM allocation a
         INNER JOIN task_config tc ON tc.id = a.task_config_id
         INNER JOIN job_config jc ON jc.id = a.job_config_id
         WHERE a.status NOT IN ('{}', '{}') AND json_valid(a.resources)
           AND json_type(a.resources, '$.devices') = 'array';",
        crate::constants::ALLOCATION_STATUS_COMPLETE,
        crate::constants::ALLOCATION_STATUS_FAILED,
    ))?;
    let rows = live.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, serde_json::Value>(2)?,
        ))
    })?;
    for row in rows {
        let (job, band, resources) = row?;
        let band = crate::structs::Band::from_stored(&band)
            .as_str()
            .to_string();
        let held = charges
            .entry(job)
            .or_default()
            .live
            .entry(band)
            .or_default();
        for (_, assignment) in crate::devices::assignments(&resources) {
            *held.entry(assignment.device).or_default() += assignment.ids.len() as f64;
        }
    }
    Ok(charges)
}

/// The device half of a submit's quota check, inside its write transaction:
/// for every governing policy's `quota.<band>.devices` key, the other jobs'
/// charge plus this job's must stay under the cap, unless this job's charge
/// does not grow (lowering a cap never breaks an admitted job).
pub fn enforce_on(
    conn: &Connection,
    governing: &[&NamespacePolicy],
    config: &JobConfig,
    namespace: &str,
) -> R<()> {
    if !governing
        .iter()
        .any(|policy| policy.quota.values().any(|limit| !limit.devices.is_empty()))
    {
        return Ok(());
    }
    let charges = job_device_charges_on(conn)?;
    let overrides = scale_overrides_on(conn)?;
    let before = charges.get(&config.name).cloned().unwrap_or_default();
    let after = JobDeviceCharge {
        declared: declared_for_config(config, &overrides),
        live: before.live.clone(),
    };
    for policy in governing {
        for (band, limit) in &policy.quota {
            for (key, cap) in &limit.devices {
                let Ok(name) = DeviceName::parse(key) else {
                    continue;
                };
                let used: f64 = charges
                    .iter()
                    .filter(|(job, _)| *job != &config.name && policy.matches(job_namespace(job)))
                    .map(|(_, charge)| charge.charged(band, &name))
                    .sum();
                let adding = after.charged(band, &name);
                let had = before.charged(band, &name);
                if used + adding > *cap && adding > had {
                    bail!(StorageError::failed_precondition(format!(
                        "namespace policy '{}' caps band '{band}' at {cap} {key} devices across \
                         {}; in use {used}, this job adds {adding}\nHint: `legion namespace get \
                         {namespace}` shows the policy; ask for fewer devices or a lower count",
                        policy.name,
                        policy.selector(),
                    )));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod quota_tests {
    use super::*;

    #[test]
    fn a_request_is_charged_under_every_compatible_key_test() {
        let mut charge = JobDeviceCharge::default();
        charge.declared.insert(
            "normal".into(),
            DeviceCounts::from([("gpu".to_string(), 2.0), ("amd/gpu".to_string(), 1.0)]),
        );
        charge.live.insert(
            "normal".into(),
            DeviceCounts::from([("nvidia/gpu/a100".to_string(), 1.0)]),
        );
        let key = |raw: &str| DeviceName::parse(raw).unwrap();
        // `gpu` could be served by an nvidia card; `amd/gpu` could not.
        assert_eq!(charge.charged("normal", &key("nvidia/gpu")), 2.0);
        assert_eq!(charge.charged("normal", &key("gpu")), 3.0);
        assert_eq!(charge.charged("normal", &key("amd/gpu")), 3.0);
        assert_eq!(charge.charged("normal", &key("fpga")), 0.0);
        assert_eq!(charge.charged("high", &key("gpu")), 0.0);
    }
}
