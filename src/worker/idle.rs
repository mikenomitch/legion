//! Idle detection: the worker half of the idle policy (suspend plan §6.7,
//! docs/suspend-and-resume.md "Idle policy").
//!
//! The worker sees every signal (datapath bytes and open ingress
//! connections, cgroup CPU, exec sessions), so it decides when an allocation
//! went quiet and reports that as `idle_since_unix` with usage. The manager
//! owns the instance row, so it decides when quiet has lasted long enough
//! and starts the suspend (`src/manager/idle.rs`).
//!
//! Everything here is pure: [`IdlePolicy::is_active`] answers "did anything
//! the task counts happen this interval" from an [`IntervalActivity`], and
//! [`IdleTracker`] turns a sequence of those answers into an idle-since time.
//! The usage sampler (`src/worker/usage.rs`) feeds them.

use crate::structs::suspend::IdleSignal;
use crate::structs::TaskConfig;
use std::collections::HashMap;

/// Below this many thousandths of a core, CPU never counts as activity,
/// whatever `cpuBelow` says. One percent of a core: a kernel thread waking or
/// a timer firing should not keep a sandbox awake, and a small task's
/// `cpuBelow` share can be smaller than that noise.
pub const MIN_CPU_ACTIVE_MILLICORES: u64 = crate::worker::usage::ACTIVE_CPU_FLOOR_MILLICORES;

/// One task's idle policy, resolved on the worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdlePolicy {
    /// `suspend.idle.after`, in seconds.
    pub after_seconds: u64,
    pub signals: Vec<IdleSignal>,
    /// The `cpu` signal's threshold: `cpuBelow` x the task's cpu, in
    /// thousandths of a core, never below [`MIN_CPU_ACTIVE_MILLICORES`].
    pub cpu_active_millicores: u64,
}

/// What happened to one allocation during one sampling interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IntervalActivity {
    /// Average CPU over the interval, thousandths of a core.
    pub cpu_millicores: u64,
    /// Bytes moved through the node datapath (egress edges or ingress
    /// connections) during the interval.
    pub network_bytes: bool,
    /// Ingress connections open at the sample.
    pub open_connections: u32,
    /// Exec sessions open at the sample.
    pub exec_sessions: u32,
}

impl IdlePolicy {
    /// The task's policy, or `None` when it declares no `suspend.idle`.
    /// `mhz_per_core` converts a `cpu` reservation in MHz to cores; without
    /// it (a platform that reports no clock) the task counts as one core.
    pub fn for_task(task: &TaskConfig, mhz_per_core: Option<f64>) -> Option<Self> {
        let idle = task.suspend.as_ref()?.resolve().ok()?.idle?;
        let cores = task_cores(&task.resources, mhz_per_core);
        let threshold = (idle.cpu_below * cores * 1000.0).round() as u64;
        Some(IdlePolicy {
            after_seconds: idle.after.as_secs(),
            signals: idle.signals,
            cpu_active_millicores: threshold.max(MIN_CPU_ACTIVE_MILLICORES),
        })
    }

    fn counts(&self, signal: IdleSignal) -> bool {
        self.signals.contains(&signal)
    }

    /// Whether anything this policy counts happened. Signals the task did
    /// not list are ignored, which is the point of listing them: a quiet
    /// open connection keeps an instance awake only with `connections`.
    pub fn is_active(&self, activity: &IntervalActivity) -> bool {
        (self.counts(IdleSignal::Network) && activity.network_bytes)
            || (self.counts(IdleSignal::Connections) && activity.open_connections > 0)
            || (self.counts(IdleSignal::Cpu)
                && activity.cpu_millicores >= self.cpu_active_millicores)
            || (self.counts(IdleSignal::Exec) && activity.exec_sessions > 0)
    }
}

/// The task's cpu in cores: declared `cores`, else `cpu` (or its `cpus`
/// alias) in MHz over the node's clock, else one core.
pub fn task_cores(resources: &serde_json::Value, mhz_per_core: Option<f64>) -> f64 {
    if let Some(cores) = crate::worker::resources::declared_cores(resources) {
        return cores;
    }
    let mhz = crate::worker::resources::cpu_weight_shares(resources).map(|mhz| mhz as f64);
    match (mhz, mhz_per_core.filter(|mhz| *mhz > 0.0)) {
        (Some(mhz), Some(per_core)) => mhz / per_core,
        _ => 1.0,
    }
}

/// This machine's average clock per core in MHz, read once. `None` where the
/// platform reports no plausible frequency (Apple Silicon reports a
/// single-digit number, some VMs report 0), matching how the scheduler
/// treats the same fingerprint (`src/resources.rs`).
pub fn local_mhz_per_core() -> Option<f64> {
    static MHZ: std::sync::OnceLock<Option<f64>> = std::sync::OnceLock::new();
    *MHZ.get_or_init(|| {
        let mut system = sysinfo::System::new();
        system.refresh_cpu_frequency();
        let frequencies: Vec<f64> = system
            .cpus()
            .iter()
            .map(|cpu| cpu.frequency() as f64)
            .collect();
        if frequencies.is_empty() || frequencies.iter().any(|mhz| *mhz < 100.0) {
            return None;
        }
        Some(frequencies.iter().sum::<f64>() / frequencies.len() as f64)
    })
}

/// Idle-since per allocation, across samples.
#[derive(Debug, Default)]
pub struct IdleTracker {
    since: HashMap<i64, i64>,
}

impl IdleTracker {
    /// Record one interval for `allocation_id` at `now_unix` and return its
    /// idle-since time: `now_unix` when the interval was active (or this is
    /// the first sample, so idleness never predates what the worker saw),
    /// otherwise the time it went quiet.
    pub fn observe(
        &mut self,
        allocation_id: i64,
        now_unix: i64,
        policy: &IdlePolicy,
        activity: &IntervalActivity,
    ) -> i64 {
        let active = policy.is_active(activity);
        let since = self.since.entry(allocation_id).or_insert(now_unix);
        if active {
            *since = now_unix;
        }
        *since
    }

    /// Forget allocations not in `live`.
    pub fn retain(&mut self, live: &std::collections::HashSet<i64>) {
        self.since.retain(|id, _| live.contains(id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn policy(signals: &[IdleSignal]) -> IdlePolicy {
        IdlePolicy {
            after_seconds: 180,
            signals: signals.to_vec(),
            cpu_active_millicores: 20,
        }
    }

    fn task(resources: serde_json::Value, idle: serde_json::Value) -> TaskConfig {
        serde_json::from_value(json!({
            "name": "workspace",
            "runtime": "sandbox",
            "config": {},
            "resources": resources,
            "stateful": { "mode": "keyed" },
            "suspend": { "idle": idle },
        }))
        .expect("task parses")
    }

    const DEFAULTS: &[IdleSignal] = &IdleSignal::DEFAULTS;

    #[test]
    fn network_bytes_count_only_with_the_network_signal() {
        let traffic = IntervalActivity {
            network_bytes: true,
            ..Default::default()
        };
        assert!(policy(&[IdleSignal::Network]).is_active(&traffic));
        assert!(!policy(&[IdleSignal::Cpu, IdleSignal::Exec]).is_active(&traffic));
    }

    /// The agent-harness case: a client holds a connection open and sends
    /// nothing. With the default signals that is idle (it would suspend
    /// mid-call), and listing `connections` keeps it awake.
    #[test]
    fn a_quiet_open_connection_counts_only_with_connections() {
        let quiet = IntervalActivity {
            open_connections: 1,
            ..Default::default()
        };
        assert!(!policy(DEFAULTS).is_active(&quiet));
        let mut with_connections = DEFAULTS.to_vec();
        with_connections.push(IdleSignal::Connections);
        assert!(policy(&with_connections).is_active(&quiet));
        // And with no connection open, `connections` alone sees nothing.
        assert!(!policy(&[IdleSignal::Connections]).is_active(&IntervalActivity::default()));
    }

    #[test]
    fn cpu_counts_at_or_above_the_threshold() {
        let at = |millicores| IntervalActivity {
            cpu_millicores: millicores,
            ..Default::default()
        };
        let cpu = policy(&[IdleSignal::Cpu]);
        assert!(!cpu.is_active(&at(19)));
        assert!(cpu.is_active(&at(20)));
        assert!(!policy(&[IdleSignal::Network]).is_active(&at(5000)));
    }

    #[test]
    fn an_open_exec_session_counts_only_with_exec() {
        let shell = IntervalActivity {
            exec_sessions: 1,
            ..Default::default()
        };
        assert!(policy(&[IdleSignal::Exec]).is_active(&shell));
        assert!(!policy(&[IdleSignal::Network, IdleSignal::Cpu]).is_active(&shell));
    }

    /// `cpuBelow` is a share of the task's own cpu: 2% of 2 cores is 40
    /// millicores. A small task's share never drops below the noise floor.
    #[test]
    fn the_cpu_threshold_scales_with_the_task() {
        let two_cores =
            IdlePolicy::for_task(&task(json!({ "cores": 2 }), json!({})), None).expect("policy");
        assert_eq!(two_cores.cpu_active_millicores, 40);
        assert_eq!(two_cores.after_seconds, 180);
        assert_eq!(two_cores.signals, DEFAULTS.to_vec());

        // 500 MHz on a 2000 MHz node is a quarter core; 2% of that is 5,
        // below the floor.
        let small = IdlePolicy::for_task(
            &task(json!({ "cpu": 500 }), json!({ "cpuBelow": 0.02 })),
            Some(2000.0),
        )
        .expect("policy");
        assert_eq!(small.cpu_active_millicores, MIN_CPU_ACTIVE_MILLICORES);

        let half = IdlePolicy::for_task(
            &task(
                json!({ "cpu": 4000 }),
                json!({ "cpuBelow": 0.5, "after": "1m" }),
            ),
            Some(2000.0),
        )
        .expect("policy");
        assert_eq!(half.cpu_active_millicores, 1000);
        assert_eq!(half.after_seconds, 60);

        let mut no_idle = task(json!({}), json!({}));
        no_idle.suspend.as_mut().unwrap().idle = None;
        assert!(IdlePolicy::for_task(&no_idle, None).is_none());
    }

    /// Idle-since starts at the first sample, holds through quiet intervals,
    /// and jumps forward on activity.
    #[test]
    fn the_tracker_holds_idle_since_through_quiet_intervals() {
        let mut tracker = IdleTracker::default();
        let policy = policy(DEFAULTS);
        let quiet = IntervalActivity::default();
        let busy = IntervalActivity {
            cpu_millicores: 500,
            ..Default::default()
        };
        assert_eq!(tracker.observe(1, 100, &policy, &quiet), 100);
        assert_eq!(tracker.observe(1, 110, &policy, &quiet), 100);
        assert_eq!(tracker.observe(1, 120, &policy, &busy), 120);
        assert_eq!(tracker.observe(1, 130, &policy, &quiet), 120);
        tracker.retain(&Default::default());
        assert_eq!(tracker.observe(1, 140, &policy, &quiet), 140);
    }
}
