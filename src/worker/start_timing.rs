//! Per-stage timings of a task start on this node (docs/metrics.md,
//! `legion_task_start_stage_seconds`).
//!
//! Work that reaches a node runs a sequence of stages before the workload is
//! up: artifact staging, volume mounts, the runtime's `prepare`, its `start`
//! (which for a sandbox is a TAP and address lease, the image layout, the
//! root disk, and the VMM spawn), then registration under the task-manager
//! lock, and finally the first passing health check. Each stage is observed
//! in the histogram as it ends, and the whole breakdown is logged once per
//! start, so a slow start can be diagnosed from one log line and a slow node
//! from one metric.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use crate::prelude::*;

/// Stage names, so a dashboard and the docs see one spelling.
pub mod stage {
    /// Artifact staging (and a restore's snapshot fetch), on its own thread.
    pub const STAGING: &str = "staging";
    /// Volume mounts (and a restore's volume rollback).
    pub const VOLUMES: &str = "volumes";
    /// Artifact consumption and the runtime's `prepare`.
    pub const PREPARE: &str = "prepare";
    /// The runtime's `start`: the process (or VMM) is spawned when it returns.
    pub const START: &str = "start";
    /// A restore from a checkpoint, in place of `start`.
    pub const RESTORE: &str = "restore";
    /// From `start` returning to the allocation being registered as started:
    /// the wait for the task-manager lock and the state-file write.
    pub const REGISTER: &str = "register";
    /// From the node receiving the work to the allocation being registered
    /// as started. Every stage above falls inside it.
    pub const TOTAL: &str = "total";
    /// From the start to the first passing check. Only for tasks with checks.
    pub const HEALTH: &str = "health";
    /// A session's wait for its workload to take work after `start`
    /// returned (the sandbox runtime: its guest agent accepting exec).
    pub const READY: &str = "ready";

    // A sandbox's own sub-stages of `start`, in order.
    /// The guest's TAP device and its address lease (TAP sandboxes only).
    pub const NETWORK: &str = "network";
    /// The kernel and the image's OCI layout and config.
    pub const IMAGE: &str = "image";
    /// The root disk: its fingerprint, then a reuse or a build.
    pub const ROOT_DISK: &str = "root_disk";
    /// Everything else the VMM needs before it runs: the checkpoint launch's
    /// device links and identity disk, the balloon, the VMM's config.
    pub const VMM_CONFIG: &str = "vmm_config";
    /// Spawning the VMM process. The guest boots after this returns.
    pub const VMM_SPAWN: &str = "vmm_spawn";
}

/// Observe one stage in `legion_task_start_stage_seconds`.
pub fn observe(runtime: &str, stage: &str, elapsed: Duration) {
    crate::manager::telemetry::worker_metrics()
        .task_start_stage_seconds
        .observe(&[runtime, stage], elapsed.as_secs_f64());
}

/// The stages one start has been through so far. Carried on the task
/// instance, so every step of the start (the worker core and the runtime
/// alike) records into the same breakdown.
#[derive(Debug, Default, Clone)]
pub struct StartTimings {
    received: Option<Instant>,
    stages: Vec<(&'static str, Duration)>,
}

impl StartTimings {
    /// A start that began now.
    pub fn begin() -> Self {
        StartTimings {
            received: Some(Instant::now()),
            stages: Vec::new(),
        }
    }

    /// Record a stage that took `elapsed`, and observe it. Outside a start
    /// (after [`Self::finish`]) the stage is observed but not kept, so an
    /// instance that is never finished again cannot grow its list.
    pub fn record(&mut self, runtime: &str, stage: &'static str, elapsed: Duration) {
        observe(runtime, stage, elapsed);
        if self.received.is_some() {
            self.stages.push((stage, elapsed));
        }
    }

    /// Run `f` as stage `stage`.
    pub fn time<T>(&mut self, runtime: &str, stage: &'static str, f: impl FnOnce() -> T) -> T {
        let started = Instant::now();
        let out = f();
        self.record(runtime, stage, started.elapsed());
        out
    }

    /// The stages recorded so far, in order.
    pub fn stages(&self) -> &[(&'static str, Duration)] {
        &self.stages
    }

    /// Close the start: observe `total` and log the breakdown once. A second
    /// call (a restart reusing the instance) does nothing.
    pub fn finish(&mut self, allocation_id: i64, task_name: &str, runtime: &str) {
        let Some(received) = self.received.take() else {
            return;
        };
        let total = received.elapsed();
        observe(runtime, stage::TOTAL, total);
        info!(
            allocation_id,
            task_name,
            task_runtime = runtime,
            total_ms = total.as_millis() as u64,
            stages = %self.summary(),
            "Task allocation started"
        );
        self.stages.clear();
    }

    /// `staging=1204ms prepare=3ms ...`, for the start log line.
    fn summary(&self) -> String {
        let mut out = String::new();
        for (stage, elapsed) in &self.stages {
            if !out.is_empty() {
                out.push(' ');
            }
            let _ = write!(out, "{stage}={}ms", elapsed.as_millis());
        }
        out
    }
}

#[cfg(test)]
mod start_timing_test {
    use super::*;

    #[test]
    fn stages_are_recorded_in_order_and_summarized() {
        let mut timings = StartTimings::begin();
        timings.record("exec", stage::PREPARE, Duration::from_millis(3));
        let value = timings.time("exec", stage::START, || 7);
        assert_eq!(value, 7);
        let names: Vec<_> = timings.stages().iter().map(|(name, _)| *name).collect();
        assert_eq!(names, [stage::PREPARE, stage::START]);
        assert!(timings.summary().starts_with("prepare=3ms start="));
    }

    #[test]
    fn finish_logs_once_and_stops_keeping_stages() {
        let mut timings = StartTimings::begin();
        timings.record("exec", stage::START, Duration::from_millis(1));
        timings.finish(1, "web", "exec");
        assert!(timings.stages().is_empty());
        // Outside a start nothing is kept, and a second finish is a no-op.
        timings.record("exec", stage::START, Duration::from_millis(1));
        timings.finish(1, "web", "exec");
        assert!(timings.stages().is_empty());
        // An instance that was never begun keeps nothing either.
        let mut idle = StartTimings::default();
        idle.record("exec", stage::START, Duration::from_millis(1));
        assert!(idle.stages().is_empty());
    }

    #[test]
    fn stages_reach_the_worker_histogram() {
        observe(
            "start-timing-test",
            stage::PREPARE,
            Duration::from_millis(2),
        );
        let rendered = crate::manager::telemetry::render_worker();
        assert!(rendered.contains(
            "legion_task_start_stage_seconds_count{runtime=\"start-timing-test\",stage=\"prepare\"} 1"
        ));
    }
}
