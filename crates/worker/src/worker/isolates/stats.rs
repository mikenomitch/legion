//! Folding host stats into the per-task aggregates a heartbeat carries
//! (brief item 11): one `IsolateTaskStats` per isolate task, summed over
//! the cordon's replicas, with rates over the last reporting interval.

use crate::structs::isolate_host::{StatsReply, TaskStats};
use crate::structs::IsolateTaskStats;
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

/// What the supervisor knows about a task that the hosts do not.
#[derive(Debug, Clone, Default)]
pub struct TaskFacts {
    pub job_id: String,
    pub task_name: String,
    pub cordon: String,
    pub hibernated: u32,
    pub vm_runs: u64,
    pub child_vms: u32,
    pub reservation_bytes: u64,
    pub problem: String,
}

/// Add one host's numbers for a task into an aggregate.
pub fn add(into: &mut IsolateTaskStats, task: &TaskStats) {
    into.resident += task.resident;
    into.active += task.active;
    into.paged_out += task.paged_out;
    into.queued += task.queued;
    into.invocations += task.invocations;
    into.cpu_micros += task.cpu_micros;
    into.refusals += task.refusals;
    into.memory_bytes += task.memory_bytes;
    into.hibernations += task.hibernations;
    for (reason, count) in &task.stopped {
        *into.stopped.entry(reason.clone()).or_default() += count;
    }
    for (limit, count) in &task.limits {
        *into.limits.entry(limit.clone()).or_default() += count;
    }
}

/// The aggregates for `facts`, from every replica's reply.
pub fn fold(facts: &[TaskFacts], replies: &[StatsReply]) -> Vec<IsolateTaskStats> {
    let mut by_task: BTreeMap<(String, String), IsolateTaskStats> = BTreeMap::new();
    for fact in facts {
        by_task.insert(
            (fact.job_id.clone(), fact.task_name.clone()),
            IsolateTaskStats {
                job_id: fact.job_id.clone(),
                task_name: fact.task_name.clone(),
                cordon: fact.cordon.clone(),
                hibernated: fact.hibernated,
                vm_runs: fact.vm_runs,
                child_vms: fact.child_vms,
                reservation_bytes: fact.reservation_bytes,
                problem: fact.problem.clone(),
                ..Default::default()
            },
        );
    }
    for reply in replies {
        for task in &reply.tasks {
            if let Some(into) = by_task.get_mut(&(task.job_id.clone(), task.task_name.clone())) {
                add(into, task);
            }
        }
    }
    by_task.into_values().collect()
}

/// Rates over the interval since the previous report, per task.
#[derive(Debug, Default)]
pub struct Rates {
    last: HashMap<(String, String), (Instant, u64, u64, u64)>,
}

impl Rates {
    /// Fill the rate fields of `stats` against the previous call. A counter
    /// that went backwards (a host restarted) restarts the rate from it.
    pub fn apply(&mut self, stats: &mut [IsolateTaskStats], now: Instant) {
        let mut next = HashMap::new();
        for task in stats.iter_mut() {
            let key = (task.job_id.clone(), task.task_name.clone());
            if let Some((at, invocations, cpu, vm_runs)) = self.last.get(&key) {
                let seconds = now.saturating_duration_since(*at).as_secs_f64();
                if seconds > 0.0 {
                    let delta = |now: u64, then: u64| now.saturating_sub(then) as f64;
                    task.invocations_per_second = delta(task.invocations, *invocations) / seconds;
                    task.cpu_seconds_per_second =
                        delta(task.cpu_micros, *cpu) / 1_000_000.0 / seconds;
                    task.vm_runs_per_minute = delta(task.vm_runs, *vm_runs) * 60.0 / seconds;
                }
            }
            next.insert(key, (now, task.invocations, task.cpu_micros, task.vm_runs));
        }
        self.last = next;
    }
}

/// Turns cumulative per-host counters into deltas for Prometheus
/// counters, surviving host restarts (a counter that drops restarted).
#[derive(Debug, Default)]
pub struct Deltas {
    last: HashMap<String, u64>,
}

impl Deltas {
    pub fn delta(&mut self, key: String, value: u64) -> u64 {
        let previous = self.last.insert(key, value).unwrap_or(0);
        if value >= previous {
            value - previous
        } else {
            value
        }
    }
}

#[cfg(test)]
mod stats_test {
    use super::*;
    use std::time::Duration;

    fn task(job: &str, resident: u32, invocations: u64, cpu: u64) -> TaskStats {
        TaskStats {
            job_id: job.into(),
            task_name: "run".into(),
            resident,
            invocations,
            cpu_micros: cpu,
            stopped: [("limits".to_string(), 2u64)].into_iter().collect(),
            limits: [("cpu".to_string(), 2u64)].into_iter().collect(),
            ..Default::default()
        }
    }

    #[test]
    fn replicas_fold_into_one_aggregate_per_task() {
        let facts = vec![
            TaskFacts {
                job_id: "a".into(),
                task_name: "run".into(),
                cordon: "default".into(),
                hibernated: 4,
                vm_runs: 7,
                child_vms: 1,
                reservation_bytes: 16 << 20,
                problem: String::new(),
            },
            TaskFacts {
                job_id: "idle".into(),
                task_name: "run".into(),
                problem: "module is still compiling".into(),
                ..Default::default()
            },
        ];
        let replies = vec![
            StatsReply {
                tasks: vec![task("a", 3, 10, 100), task("stranger", 9, 9, 9)],
                ..Default::default()
            },
            StatsReply {
                tasks: vec![task("a", 2, 5, 50)],
                ..Default::default()
            },
        ];
        let folded = fold(&facts, &replies);
        assert_eq!(
            folded.len(),
            2,
            "tasks the supervisor does not know are dropped"
        );
        let a = &folded[0];
        assert_eq!((a.resident, a.invocations, a.cpu_micros), (5, 15, 150));
        assert_eq!((a.hibernated, a.vm_runs, a.child_vms), (4, 7, 1));
        assert_eq!(a.stopped["limits"], 4);
        assert_eq!(a.limits["cpu"], 4);
        assert_eq!(folded[1].problem, "module is still compiling");
        assert_eq!(folded[1].resident, 0);
    }

    #[test]
    fn rates_over_the_interval() {
        let mut rates = Rates::default();
        let start = Instant::now();
        let mut stats = vec![IsolateTaskStats {
            job_id: "a".into(),
            task_name: "run".into(),
            invocations: 100,
            cpu_micros: 1_000_000,
            vm_runs: 1,
            ..Default::default()
        }];
        rates.apply(&mut stats, start);
        assert_eq!(
            stats[0].invocations_per_second, 0.0,
            "no rate on the first report"
        );
        stats[0].invocations = 300;
        stats[0].cpu_micros = 3_000_000;
        stats[0].vm_runs = 3;
        rates.apply(&mut stats, start + Duration::from_secs(10));
        assert!((stats[0].invocations_per_second - 20.0).abs() < 1e-9);
        assert!((stats[0].cpu_seconds_per_second - 0.2).abs() < 1e-9);
        assert!((stats[0].vm_runs_per_minute - 12.0).abs() < 1e-9);
        // A host restart drops the counter: no negative rate.
        stats[0].invocations = 5;
        rates.apply(&mut stats, start + Duration::from_secs(20));
        assert_eq!(stats[0].invocations_per_second, 0.0);
    }

    #[test]
    fn deltas_survive_restarts() {
        let mut deltas = Deltas::default();
        assert_eq!(deltas.delta("k".into(), 10), 10);
        assert_eq!(deltas.delta("k".into(), 15), 5);
        assert_eq!(
            deltas.delta("k".into(), 3),
            3,
            "a restarted host counts from zero"
        );
    }
}
