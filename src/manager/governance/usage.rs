//! Time-integrated usage accounting (docs/usage-accounting.md).
//!
//! Every `governance.usage.interval` the manager leading the status lane
//! reads the allocations holding node capacity — the set namespace quotas
//! charge — and adds each one's reservation times the elapsed seconds into
//! the hourly `usage_rollup` bucket for its (namespace, job, submitter).
//! Dimensions:
//!
//! - `cpu_mhz` (MHz-seconds), `memory_mib` (MiB-seconds), `cores`
//!   (core-seconds), `allocations` (allocation-seconds);
//! - `resource:<name>` for every other numeric reservation key (custom
//!   resources; devices once they are modeled), so a new resource is
//!   accounted without a schema change.
//!
//! This integrates *reservations*, not measured consumption: an idle task
//! reserving 4 cores is charged 4 cores. The accumulator keeps its last-tick
//! time in memory; after a status-lane leadership change the new leader's
//! first tick only sets its baseline, so up to one interval per failover is
//! not accounted, and an interval is credited wholly to the hour it ends
//! in.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::prelude::*;
use crate::storage::governance_store::{HeldAllocation, UsageRow};
use crate::storage::store::Store;
use crate::structs::governance::GovernanceConfig;

/// One hour in milliseconds: the bucket width.
pub const BUCKET_MS: i64 = 3_600_000;

/// The start of the hourly bucket containing `ms`.
pub fn bucket_of(ms: i64) -> i64 {
    ms.div_euclid(BUCKET_MS) * BUCKET_MS
}

/// The dimensions of one allocation's reservation, in units (not yet
/// multiplied by time).
pub fn dimensions(resources: &Value) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    out.insert("allocations".to_string(), 1.0);
    let Some(object) = resources.as_object() else {
        return out;
    };
    for (key, value) in object {
        let amount = match (key.as_str(), value) {
            ("cores", Value::Array(cores)) => cores.len() as f64,
            (_, Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => continue,
        };
        if amount == 0.0 {
            continue;
        }
        let dimension = match key.as_str() {
            "cpu" => "cpu_mhz".to_string(),
            "memory" => "memory_mib".to_string(),
            "cores" => "cores".to_string(),
            // The oversubscription ceiling is not a reservation.
            "memoryMax" | "memory_max" => continue,
            other => format!("resource:{other}"),
        };
        *out.entry(dimension).or_default() += amount;
    }
    out
}

/// The rollup rows for `held` allocations over `elapsed_secs`, credited to
/// the bucket containing `now_ms`.
pub fn integrate(
    held: &[HeldAllocation],
    submitters: &HashMap<String, String>,
    elapsed_secs: f64,
    now_ms: i64,
) -> Vec<UsageRow> {
    let bucket_ms = bucket_of(now_ms);
    let mut sums: BTreeMap<(String, String, String, String), f64> = BTreeMap::new();
    for allocation in held {
        let namespace = crate::structs::namespace::job_namespace(&allocation.job_id).to_string();
        let submitter = submitters
            .get(&allocation.job_id)
            .cloned()
            .unwrap_or_default();
        for (dimension, units) in dimensions(&allocation.resources) {
            *sums
                .entry((
                    namespace.clone(),
                    allocation.job_id.clone(),
                    submitter.clone(),
                    dimension,
                ))
                .or_default() += units * elapsed_secs;
        }
    }
    sums.into_iter()
        .map(
            |((namespace, job_id, submitter, dimension), amount)| UsageRow {
                bucket_ms,
                namespace,
                job_id,
                submitter,
                dimension,
                amount,
            },
        )
        .collect()
}

/// One accumulation step: integrate what is held now over `elapsed_secs`
/// into the bucket containing `now_ms`, and drop buckets older than
/// `retention`. Returns the rows written. The accumulator loop calls this;
/// tests call it with simulated time.
pub fn accumulate_once(
    store: &Store,
    elapsed_secs: f64,
    now_ms: i64,
    retention: Duration,
) -> R<usize> {
    let held = store.held_allocations()?;
    let submitters = store.job_submitters().unwrap_or_default();
    let rows = integrate(&held, &submitters, elapsed_secs, now_ms);
    let count = rows.len();
    store.accumulate_usage(rows, now_ms - retention.as_millis() as i64)?;
    Ok(count)
}

/// Starts the accumulator on the current runtime. It idles on managers
/// that do not lead the status lane.
pub fn start(store: Store, config: &GovernanceConfig) -> tokio::task::JoinHandle<()> {
    let interval = config.usage_interval().max(Duration::from_secs(1));
    let retention = config.usage_retention();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_ms: Option<i64> = None;
        loop {
            ticker.tick().await;
            if !store.is_raft_leader(crate::storage::Lane::Status) {
                last_ms = None;
                continue;
            }
            let now = super::now_ms();
            let Some(previous) = last_ms.replace(now) else {
                continue;
            };
            // A stalled tick is credited at most two intervals.
            let elapsed_ms = (now - previous).clamp(0, 2 * interval.as_millis() as i64);
            let store = store.clone();
            let result = tokio::task::spawn_blocking(move || {
                accumulate_once(&store, elapsed_ms as f64 / 1000.0, now, retention)
            })
            .await;
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => debug!(error = %err, "usage accumulation failed"),
                Err(err) => warn!(error = %err, "usage accumulation task failed"),
            }
        }
    })
}

#[cfg(test)]
mod usage_test {
    use super::*;
    use serde_json::json;

    #[test]
    fn dimensions_cover_core_and_custom_resources_test() {
        let dims = dimensions(&json!({
            "cpu": 500, "memory": 256, "cores": [0, 1], "gpu": 1, "memoryMax": 1024, "label": "x"
        }));
        assert_eq!(dims["cpu_mhz"], 500.0);
        assert_eq!(dims["memory_mib"], 256.0);
        assert_eq!(dims["cores"], 2.0);
        assert_eq!(dims["resource:gpu"], 1.0);
        assert_eq!(dims["allocations"], 1.0);
        assert!(!dims.contains_key("resource:memoryMax"));
        assert!(!dims.contains_key("resource:label"));
    }

    #[test]
    fn integrate_sums_per_job_and_buckets_by_hour_test() {
        let held = vec![
            HeldAllocation {
                job_id: "team/web".into(),
                resources: json!({ "cpu": 1000, "memory": 512 }),
            },
            HeldAllocation {
                job_id: "team/web".into(),
                resources: json!({ "cpu": 1000, "memory": 512 }),
            },
            HeldAllocation {
                job_id: "api".into(),
                resources: json!({ "cpu": 100 }),
            },
        ];
        let submitters = HashMap::from([("team/web".to_string(), "acc-1".to_string())]);
        let rows = integrate(&held, &submitters, 60.0, BUCKET_MS + 5);
        let web_cpu = rows
            .iter()
            .find(|r| r.job_id == "team/web" && r.dimension == "cpu_mhz")
            .unwrap();
        assert_eq!(web_cpu.amount, 2.0 * 1000.0 * 60.0);
        assert_eq!(web_cpu.namespace, "team");
        assert_eq!(web_cpu.submitter, "acc-1");
        assert_eq!(web_cpu.bucket_ms, BUCKET_MS);
        let api = rows
            .iter()
            .find(|r| r.job_id == "api" && r.dimension == "allocations")
            .unwrap();
        assert_eq!(api.namespace, "default");
        assert_eq!(api.amount, 60.0);
        assert_eq!(bucket_of(BUCKET_MS - 1), 0);
    }
}
