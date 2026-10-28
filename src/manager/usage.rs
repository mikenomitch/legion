//! Measured usage on the manager: the latest report each node sent, held in
//! memory beside liveness, and the `GetNodeUsage` reply built from it.
//!
//! Usage is high-churn and worthless after a restart (the next report
//! replaces it within one interval), so it never touches the state store, for
//! the same reason heartbeat timestamps stay off the raft path
//! (docs/node-liveness.md). Workers send it on the heartbeat
//! (`HeartbeatRequest.usage`, see `src/worker/usage.rs`).
//!
//! Everything here is pure over its inputs so the reply arithmetic is tested
//! without a store or a cluster.

use crate::resources::{NodeCapacity, OversubscribeRatios, ResourcePolicy, MEMORY_MAX_KEY};
use crate::structs::{
    Allocation, AllocationUsage, Band, GetNodeUsageReply, Node, NodeAllocationUsage,
    NodeResourceUsage, SuspendedCapacity, UsageReport,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Reports older than this are dropped by [`UsageTable::sweep`]: twelve
/// missed report intervals, so a node that stopped reporting (lost,
/// enforcement turned off, decommissioned) stops showing numbers that look
/// current, and its metric series drop to zero.
pub const USAGE_STALE_AFTER: Duration = Duration::from_secs(120);

/// How often the heartbeat path runs [`UsageTable::sweep`]. A sweep walks
/// every node once, so it is amortized rather than run per heartbeat.
const SWEEP_EVERY: Duration = Duration::from_secs(60);

const BYTES_PER_MIB: f64 = 1024.0 * 1024.0;

/// One allocation the idle sweep should suspend (see
/// [`UsageTable::idle_due`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdleDue {
    pub node_id: String,
    pub allocation_id: i64,
    pub idle_since_unix: i64,
    pub idle_after_seconds: u64,
}

/// One node's latest report and when this manager received it.
#[derive(Debug, Clone)]
pub struct NodeUsageEntry {
    pub report: UsageReport,
    pub received_at: Instant,
}

/// Latest usage report per node, plus an allocation index so instance reads
/// can find their allocation's usage without knowing its node.
#[derive(Debug, Default)]
pub struct UsageTable {
    nodes: HashMap<String, NodeUsageEntry>,
    by_allocation: HashMap<i64, String>,
    last_sweep: Option<Instant>,
}

impl UsageTable {
    /// Replace a node's report. Allocations the previous report listed and
    /// this one does not are forgotten.
    pub fn ingest(&mut self, node_id: &str, report: UsageReport, now: Instant) {
        if let Some(previous) = self.nodes.get(node_id) {
            for usage in &previous.report.allocations {
                self.by_allocation.remove(&usage.allocation_id);
            }
        }
        for usage in &report.allocations {
            self.by_allocation
                .insert(usage.allocation_id, node_id.to_string());
        }
        publish_node_gauges(node_id, Some(&report));
        self.nodes.insert(
            node_id.to_string(),
            NodeUsageEntry {
                report,
                received_at: now,
            },
        );
    }

    pub fn node(&self, node_id: &str) -> Option<&NodeUsageEntry> {
        self.nodes.get(node_id)
    }

    /// The latest usage of one allocation, wherever it runs.
    pub fn allocation(&self, allocation_id: i64) -> Option<&AllocationUsage> {
        let node_id = self.by_allocation.get(&allocation_id)?;
        self.nodes
            .get(node_id)?
            .report
            .allocations
            .iter()
            .find(|usage| usage.allocation_id == allocation_id)
    }

    /// Allocations whose idle policy has run out at `now_unix`: idle since
    /// at least `idle_after_seconds` ago by their node's latest report. Pure
    /// over the table, so the idle sweep costs no store reads until it has
    /// something to suspend. Sorted by node then allocation, for a stable
    /// sweep order.
    pub fn idle_due(&self, now_unix: i64) -> Vec<IdleDue> {
        let mut due: Vec<IdleDue> = self
            .nodes
            .iter()
            .flat_map(|(node_id, entry)| {
                entry.report.allocations.iter().filter_map(move |usage| {
                    let after = i64::try_from(usage.idle_after_seconds).ok()?;
                    (after > 0
                        && usage.idle_since_unix > 0
                        && now_unix.saturating_sub(usage.idle_since_unix) >= after)
                        .then(|| IdleDue {
                            node_id: node_id.clone(),
                            allocation_id: usage.allocation_id,
                            idle_since_unix: usage.idle_since_unix,
                            idle_after_seconds: usage.idle_after_seconds,
                        })
                })
            })
            .collect();
        due.sort_by(|a, b| {
            a.node_id
                .cmp(&b.node_id)
                .then(a.allocation_id.cmp(&b.allocation_id))
        });
        due
    }

    /// Every node's latest report received within `max_age` of `now`, as the
    /// scaling-input rollup folds it (`crate::manager::scaling_inputs`).
    /// One pass over the table; the caller holds the lock only for this.
    pub fn current_samples(
        &self,
        now: Instant,
        max_age: Duration,
    ) -> Vec<crate::manager::scaling_inputs::NodeSample> {
        self.nodes
            .iter()
            .filter(|(_, entry)| now.saturating_duration_since(entry.received_at) <= max_age)
            .map(|(node_id, entry)| {
                crate::manager::scaling_inputs::NodeSample::from_report(node_id, &entry.report)
            })
            .collect()
    }

    /// Whether `node_id` has a current usage report. A node without one
    /// cannot run an idle policy (no worker-side activity tracking).
    pub fn reports_for(&self, node_id: &str) -> bool {
        self.nodes.contains_key(node_id)
    }

    /// Drop reports older than [`USAGE_STALE_AFTER`], at most once per
    /// [`SWEEP_EVERY`]. Returns the node ids dropped.
    pub fn sweep(&mut self, now: Instant) -> Vec<String> {
        if self
            .last_sweep
            .is_some_and(|last| now.duration_since(last) < SWEEP_EVERY)
        {
            return Vec::new();
        }
        self.last_sweep = Some(now);
        let stale: Vec<String> = self
            .nodes
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.received_at) >= USAGE_STALE_AFTER)
            .map(|(node_id, _)| node_id.clone())
            .collect();
        for node_id in &stale {
            if let Some(entry) = self.nodes.remove(node_id) {
                for usage in &entry.report.allocations {
                    self.by_allocation.remove(&usage.allocation_id);
                }
            }
            publish_node_gauges(node_id, None);
        }
        stale
    }
}

/// Per-node gauges (docs/metrics.md, "Node usage"). `None` zeroes the
/// node's series, which is how a node that stopped reporting reads.
fn publish_node_gauges(node_id: &str, report: Option<&UsageReport>) {
    let metrics = crate::manager::telemetry::metrics();
    let labels = [node_id];
    let memory = report.and_then(|report| report.node_memory.as_ref());
    let (used, cpu) = report
        .map(|report| {
            report.allocations.iter().fold((0u64, 0u64), |acc, usage| {
                (
                    acc.0.saturating_add(usage.memory_current_bytes),
                    acc.1.saturating_add(usage.cpu_millicores),
                )
            })
        })
        .unwrap_or((0, 0));
    let clamp = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
    metrics
        .node_memory_physical_bytes
        .set(&labels, clamp(memory.map_or(0, |m| m.total_bytes)));
    metrics
        .node_memory_available_bytes
        .set(&labels, clamp(memory.map_or(0, |m| m.available_bytes)));
    metrics.node_memory_used_bytes.set(&labels, clamp(used));
    metrics.node_cpu_used_millicores.set(&labels, clamp(cpu));
}

/// Build the `GetNodeUsage` reply for one node from its stored row, the
/// allocations placed on it, and (when there is one) its latest report.
///
/// Capacity and reservations use the scheduler's own arithmetic
/// ([`NodeCapacity`], [`ResourcePolicy::usage_request`]), so the numbers
/// here are the numbers placement packed against. `bands` maps job config
/// ids to their priority band, for the pressure-order columns; a missing
/// entry shows the default band.
pub fn build_node_usage_reply(
    node: &Node,
    allocations: &[Allocation],
    policy: &ResourcePolicy,
    bands: &HashMap<String, Band>,
    entry: Option<&NodeUsageEntry>,
    now: Instant,
) -> GetNodeUsageReply {
    let capacity = NodeCapacity::of(node).unwrap_or(NodeCapacity {
        memory_mib: None,
        cpu_mhz: None,
        mhz_per_core: None,
        cpu_cores: None,
        ratios: OversubscribeRatios::default(),
    });
    let report = entry.map(|entry| &entry.report);
    let usage_by_id: HashMap<i64, &AllocationUsage> = report
        .map(|report| {
            report
                .allocations
                .iter()
                .map(|usage| (usage.allocation_id, usage))
                .collect()
        })
        .unwrap_or_default();

    let mut rows: Vec<NodeAllocationUsage> = allocations
        .iter()
        .map(|allocation| {
            let request = policy.usage_request(&allocation.resources).ok();
            let reserved = |name: &str| {
                request
                    .as_ref()
                    .and_then(|request| request.amount(name))
                    .unwrap_or(0.0)
            };
            let usage = usage_by_id
                .get(&allocation.id)
                .map(|usage| (*usage).clone());
            let idle_seconds = match (&usage, report) {
                (Some(usage), Some(report)) => {
                    (report.sampled_at_unix_ms / 1000 - usage.last_activity_unix).max(0)
                }
                _ => 0,
            };
            let (pressure_rank, pressure_steps) = usage
                .as_ref()
                .map(|usage| (usage.pressure_rank, usage.pressure_steps.clone()))
                .unwrap_or_default();
            NodeAllocationUsage {
                allocation_id: allocation.id,
                job_id: allocation.job_id.clone(),
                instance_id: allocation.instance_id.clone(),
                reserved_memory_mib: reserved("memory"),
                reserved_cpu_mhz: reserved("cpu"),
                usage,
                idle_seconds,
                memory_max_mib: allocation
                    .resources
                    .get(MEMORY_MAX_KEY)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0),
                band: bands
                    .get(&allocation.job_config_id)
                    .copied()
                    .unwrap_or_default()
                    .as_str()
                    .to_string(),
                pressure_rank,
                pressure_steps,
                suspended: allocation.status == crate::constants::ALLOCATION_STATUS_SUSPENDED,
            }
        })
        .collect();
    rows.sort_by_key(|row| row.allocation_id);
    // Held charges of suspended instances: already inside the reserved sums
    // below, summarized apart so the CLI can say how much of "reserved" is
    // suspended work. The instance count, checkpoint bytes, and budget need
    // the store, so `fill_suspended_capacity` adds them.
    let held = rows.iter().filter(|row| row.suspended);
    let suspended = SuspendedCapacity {
        holding: held.clone().count() as u32,
        held_memory_mib: held.clone().map(|row| row.reserved_memory_mib).sum(),
        held_cpu_mhz: held.map(|row| row.reserved_cpu_mhz).sum(),
        ..Default::default()
    };

    let reserved_memory: f64 = rows.iter().map(|row| row.reserved_memory_mib).sum();
    let reserved_cpu: f64 = rows.iter().map(|row| row.reserved_cpu_mhz).sum();
    let measured = rows.iter().filter_map(|row| row.usage.as_ref());
    let (used_bytes, used_millicores) = measured.fold((0u64, 0u64), |acc, usage| {
        (
            acc.0.saturating_add(usage.memory_current_bytes),
            acc.1.saturating_add(usage.cpu_millicores),
        )
    });
    let node_memory = report.and_then(|report| report.node_memory.as_ref());

    GetNodeUsageReply {
        node_id: node.id.clone(),
        status: node.status.clone(),
        usage_reported: report.is_some(),
        sampled_at_unix_ms: report.map_or(0, |report| report.sampled_at_unix_ms),
        report_age_seconds: entry.map_or(0, |entry| {
            now.duration_since(entry.received_at).as_secs() as i64
        }),
        memory: Some(NodeResourceUsage {
            capacity: capacity.memory_mib.unwrap_or(0.0),
            reserved: reserved_memory,
            used: used_bytes as f64 / BYTES_PER_MIB,
            physical_bytes: node_memory.map_or(0, |memory| memory.total_bytes),
            available_bytes: node_memory.map_or(0, |memory| memory.available_bytes),
            pressure_some_avg10: node_memory.map_or(0.0, |memory| memory.psi_some_avg10),
            pressure_available: node_memory.is_some_and(|memory| memory.psi_available),
            oversubscribe_ratio: capacity.ratios.memory,
            pressure_response: report.and_then(|report| report.pressure.clone()),
            ..Default::default()
        }),
        cpu: Some(NodeResourceUsage {
            capacity: capacity.cpu_mhz.unwrap_or(0.0),
            reserved: reserved_cpu,
            used: capacity
                .mhz_per_core
                .map_or(0.0, |mhz| used_millicores as f64 / 1000.0 * mhz),
            oversubscribe_ratio: capacity.ratios.cpu,
            cores: capacity.cpu_cores.unwrap_or(0.0),
            used_millicores,
            ..Default::default()
        }),
        allocations: rows,
        suspended: Some(suspended),
    }
}

/// Complete a reply's [`SuspendedCapacity`] with what only the store and the
/// content index know: how many suspended instances keep their checkpoint
/// on the node, those checkpoints' bytes, and the node's last reported
/// `snapshots` budget. Separate from [`build_node_usage_reply`] so that
/// function stays pure over its inputs.
pub fn fill_suspended_capacity(
    reply: &mut GetNodeUsageReply,
    suspended_on_node: (u32, u64),
    snapshot_budget: Option<crate::structs::SnapshotBudget>,
) {
    let capacity = reply.suspended.get_or_insert_with(Default::default);
    capacity.instances = suspended_on_node.0.max(capacity.holding);
    capacity.checkpoint_bytes = suspended_on_node.1;
    capacity.snapshot_budget = snapshot_budget;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::NodeMemoryUsage;
    use serde_json::json;

    const MIB: u64 = 1024 * 1024;

    fn report(sampled_at_unix_ms: i64, allocations: Vec<AllocationUsage>) -> UsageReport {
        UsageReport {
            sampled_at_unix_ms,
            allocations,
            node_memory: Some(NodeMemoryUsage {
                total_bytes: 8192 * MIB,
                available_bytes: 6000 * MIB,
                psi_some_avg10: 0.4,
                psi_available: true,
            }),
            pressure: None,
        }
    }

    fn usage(allocation_id: i64, memory_mib: u64, last_activity_unix: i64) -> AllocationUsage {
        AllocationUsage {
            allocation_id,
            memory_current_bytes: memory_mib * MIB,
            memory_peak_bytes: memory_mib * MIB,
            memory_anon_bytes: memory_mib * MIB / 2,
            cpu_usage_usec: 0,
            cpu_millicores: 500,
            psi_memory_some_avg10: 0.0,
            last_activity_unix,
            pressure_rank: 0,
            pressure_steps: Vec::new(),
            idle_since_unix: 0,
            idle_after_seconds: 0,
        }
    }

    /// Due once quiet for `after`, never without a policy, never early, and
    /// in a stable order across nodes.
    #[test]
    fn idle_due_lists_allocations_past_their_window() {
        let mut table = UsageTable::default();
        let now = Instant::now();
        let idle = |id, since, after| AllocationUsage {
            idle_since_unix: since,
            idle_after_seconds: after,
            ..usage(id, 10, since)
        };
        table.ingest(
            "w-2",
            report(0, vec![idle(5, 1_000, 180), idle(6, 1_100, 180)]),
            now,
        );
        table.ingest(
            "w-1",
            report(0, vec![idle(1, 1_000, 60), usage(2, 10, 0), idle(3, 0, 60)]),
            now,
        );
        let due: Vec<(String, i64)> = table
            .idle_due(1_180)
            .into_iter()
            .map(|due| (due.node_id, due.allocation_id))
            .collect();
        assert_eq!(
            due,
            vec![("w-1".to_string(), 1), ("w-2".to_string(), 5)],
            "6 has 100s to go; 2 has no policy; 3 has no idle-since yet"
        );
        assert!(table.idle_due(1_059).is_empty());
        assert!(table.reports_for("w-1") && !table.reports_for("w-9"));
    }

    fn allocation(id: i64, resources: serde_json::Value) -> Allocation {
        Allocation {
            id,
            status: "running".into(),
            resources,
            node_id: "w-1".into(),
            job_config_id: "cfg".into(),
            task_config_id: 1,
            job_id: "web".into(),
            instance_id: None,
            network: None,
        }
    }

    fn node() -> Node {
        Node {
            id: "w-1".into(),
            status: "ready".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: json!({}),
            fingerprint_attrs: json!({
                "memory_total": (8192 * MIB).to_string(),
                "cpus": {
                    "0": { "frequency": "2000" },
                    "1": { "frequency": "2000" },
                },
            }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn policy() -> ResourcePolicy {
        ResourcePolicy::from_manager_config(&crate::structs::config::ManagerConfigFull::default())
            .expect("policy")
    }

    #[test]
    fn ingest_replaces_a_nodes_report_and_its_allocation_index() {
        let mut table = UsageTable::default();
        let now = Instant::now();
        table.ingest(
            "w-1",
            report(0, vec![usage(1, 100, 0), usage(2, 50, 0)]),
            now,
        );
        assert!(table.allocation(2).is_some());

        table.ingest("w-1", report(10_000, vec![usage(1, 120, 0)]), now);
        assert_eq!(
            table.allocation(1).map(|usage| usage.memory_current_bytes),
            Some(120 * MIB)
        );
        assert!(
            table.allocation(2).is_none(),
            "an allocation the newer report omits is gone"
        );
    }

    #[test]
    fn sweep_drops_stale_reports_at_most_once_a_minute() {
        let mut table = UsageTable::default();
        let start = Instant::now();
        table.ingest("old", report(0, vec![usage(1, 10, 0)]), start);
        assert!(table.sweep(start).is_empty(), "nothing is stale yet");

        let later = start + USAGE_STALE_AFTER;
        table.ingest("fresh", report(0, vec![]), later);
        assert_eq!(
            table.sweep(later + Duration::from_secs(1)),
            vec!["old".to_string()]
        );
        assert!(table.node("old").is_none());
        assert!(table.allocation(1).is_none());
        assert!(table.node("fresh").is_some());

        // A second sweep inside the minute does no work, even with a stale
        // entry present.
        table.ingest("stale-again", report(0, vec![]), start);
        assert!(table.sweep(later + Duration::from_secs(2)).is_empty());
    }

    #[test]
    fn the_reply_joins_reservations_with_measured_usage() {
        let policy = policy();
        let allocations = vec![
            allocation(2, json!({ "memory": 512, "cpu": 1000 })),
            allocation(1, json!({ "memory": 256, "cpu": 500 })),
        ];
        let now = Instant::now();
        let entry = NodeUsageEntry {
            // Allocation 1 last active at 100s, sampled at 160s: idle 60s.
            report: report(160_000, vec![usage(1, 300, 100)]),
            received_at: now,
        };
        let reply = build_node_usage_reply(
            &node(),
            &allocations,
            &policy,
            &HashMap::new(),
            Some(&entry),
            now + Duration::from_secs(3),
        );

        assert!(reply.usage_reported);
        assert_eq!(reply.report_age_seconds, 3);
        assert_eq!(
            reply
                .allocations
                .iter()
                .map(|row| row.allocation_id)
                .collect::<Vec<_>>(),
            vec![1, 2],
            "ordered by allocation id"
        );
        assert_eq!(reply.allocations[0].idle_seconds, 60);
        assert!(reply.allocations[1].usage.is_none(), "not measured");

        let memory = reply.memory.expect("memory");
        assert_eq!(memory.capacity, 8192.0);
        assert_eq!(memory.reserved, 768.0);
        assert_eq!(memory.used, 300.0);
        assert_eq!(memory.available_bytes, 6000 * MIB);
        assert!(memory.pressure_available);

        let cpu = reply.cpu.expect("cpu");
        assert_eq!(cpu.capacity, 4000.0);
        assert_eq!(cpu.reserved, 1500.0);
        // Half a core at 2000 MHz per core.
        assert_eq!(cpu.used, 1000.0);
    }

    /// A node that never reported (enforcement off, an old worker) still
    /// gets capacity and reservations, and says it has no usage.
    #[test]
    fn an_unreported_node_still_shows_capacity_and_reservations() {
        let reply = build_node_usage_reply(
            &node(),
            &[allocation(1, json!({ "memory": 256 }))],
            &policy(),
            &HashMap::new(),
            None,
            Instant::now(),
        );
        assert!(!reply.usage_reported);
        assert_eq!(reply.sampled_at_unix_ms, 0);
        let memory = reply.memory.expect("memory");
        assert_eq!(memory.reserved, 256.0);
        assert_eq!(memory.used, 0.0);
        assert!(reply.allocations[0].usage.is_none());
    }
}
