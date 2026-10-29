//! What the autoscaler app reports (`ReportScalingStatus`; docs/autoscaling.md,
//! "Knowing whether the autoscaler is running").
//!
//! Each replica of the app sends one report per evaluation tick: its
//! instance, interval, mode, the shards it holds, and the last evaluation
//! of every target it evaluated. The manager that leads the status lane
//! holds the newest report per replica and per target in memory, like usage
//! rollups: nothing is replicated, a failover starts empty, and the next
//! reports (one interval later) refill it. `GetScalingStatus` reads it
//! back:
//!
//! - **The app is running while any replica reports.** It reads NOT
//!   RUNNING once three of the newest reporter's intervals pass with no
//!   report from any replica, so `legion scaling status` works, and is
//!   honest, with the app down.
//! - **Each target is owned by one replica**: its entry is the newest
//!   report of it, and names the replica that sent it. A replica that dies
//!   leaves its targets' entries to go stale until the replica that takes
//!   its shards reports them.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::structs::{ReportScalingStatusRequest, ScalingTargetReport};

/// Intervals without a report before the app counts as not running, and
/// before a target's last evaluation counts as stale.
pub const MISSED_INTERVALS: u32 = 3;

/// The interval assumed before any report says otherwise (the app's
/// default tick).
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(15);

/// Targets (and replicas) not reported for this long are forgotten (a
/// removed policy, a replica that is gone).
const TARGET_RETENTION: Duration = Duration::from_secs(3600);

/// The newest report from one replica of the app.
#[derive(Debug, Clone)]
pub struct AppReport {
    pub instance: String,
    pub interval: Duration,
    pub mode: String,
    pub version: String,
    pub received: Instant,
    pub received_unix_ms: i64,
    /// The shards it holds (empty: all of them).
    pub shards: Vec<u32>,
    /// How many shards policies hash to (0: the replica does not shard).
    pub shard_count: u32,
    /// Targets in its last report.
    pub targets: u32,
}

impl AppReport {
    /// Whether this replica reported within [`MISSED_INTERVALS`] of
    /// `interval` at `now`. Pass the app's interval
    /// ([`ScalingStatusBoard::interval`], the newest report's): every
    /// replica runs the same config, and a replica replaced by a config
    /// change must not count as running on its old, longer interval.
    pub fn fresh(&self, now: Instant, interval: Duration) -> bool {
        now.saturating_duration_since(self.received) < interval * MISSED_INTERVALS
    }
}

/// The newest evaluation of one target.
#[derive(Debug, Clone)]
pub struct TargetEntry {
    pub state: String,
    pub current: u32,
    pub want: u32,
    pub evaluated_at_unix_ms: i64,
    pub summary: String,
    /// The last full decision the app sent (kept when a report omits it as
    /// unchanged).
    pub decision_json: String,
    pub policy_digest: String,
    pub received: Instant,
    /// The replica that reported it.
    pub instance: String,
}

/// Whether the app is running, as the reports show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppLiveness {
    /// A replica reported within [`MISSED_INTERVALS`] of its interval.
    Running,
    /// None did.
    NotRunning,
    /// No report yet, and this manager has not held the board for
    /// [`MISSED_INTERVALS`] default intervals: too early to tell.
    Starting,
}

#[derive(Debug)]
struct Inner {
    created: Instant,
    apps: HashMap<String, AppReport>,
    targets: HashMap<(String, String, String), TargetEntry>,
}

/// The board (one per manager process; see the module docs).
#[derive(Debug)]
pub struct ScalingStatusBoard {
    inner: Mutex<Inner>,
}

impl Default for ScalingStatusBoard {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner {
                created: Instant::now(),
                apps: HashMap::new(),
                targets: HashMap::new(),
            }),
        }
    }
}

fn key(job_id: &str, group: &str, task: &str) -> (String, String, String) {
    (job_id.to_string(), group.to_string(), task.to_string())
}

impl ScalingStatusBoard {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Take one report. Returns how many targets it carried.
    pub fn record(
        &self,
        report: &ReportScalingStatusRequest,
        now: Instant,
        now_unix_ms: i64,
    ) -> u32 {
        let mut inner = self.lock();
        let interval = Duration::from_secs(u64::from(report.interval_seconds.max(1)));
        inner.apps.insert(
            report.instance.clone(),
            AppReport {
                instance: report.instance.clone(),
                interval,
                mode: report.mode.clone(),
                version: report.version.clone(),
                received: now,
                received_unix_ms: now_unix_ms,
                shards: report.shards.clone(),
                shard_count: report.shard_count,
                targets: report.targets.len() as u32,
            },
        );
        for target in &report.targets {
            Self::record_target(&mut inner, &report.instance, target, now);
        }
        inner
            .targets
            .retain(|_, entry| now.saturating_duration_since(entry.received) < TARGET_RETENTION);
        inner
            .apps
            .retain(|_, app| now.saturating_duration_since(app.received) < TARGET_RETENTION);
        report.targets.len() as u32
    }

    fn record_target(
        inner: &mut Inner,
        instance: &str,
        target: &ScalingTargetReport,
        now: Instant,
    ) {
        let key = key(&target.job_id, &target.task_group, &target.task_name);
        let decision_json = if target.decision_json.is_empty() {
            inner
                .targets
                .get(&key)
                .map(|entry| entry.decision_json.clone())
                .unwrap_or_default()
        } else {
            target.decision_json.clone()
        };
        inner.targets.insert(
            key,
            TargetEntry {
                state: target.state.clone(),
                current: target.current,
                want: target.want,
                evaluated_at_unix_ms: target.evaluated_at_unix_ms,
                summary: target.summary.clone(),
                decision_json,
                policy_digest: target.policy_digest.clone(),
                received: now,
                instance: instance.to_string(),
            },
        );
    }

    /// The newest report from any replica, if any.
    pub fn app(&self) -> Option<AppReport> {
        self.lock()
            .apps
            .values()
            .max_by_key(|app| app.received)
            .cloned()
    }

    /// Every replica that reported within the retention, newest first.
    pub fn replicas(&self) -> Vec<AppReport> {
        let mut apps: Vec<AppReport> = self.lock().apps.values().cloned().collect();
        apps.sort_by(|a, b| {
            b.received
                .cmp(&a.received)
                .then(a.instance.cmp(&b.instance))
        });
        apps
    }

    /// The app's interval: the newest reporter's, or the default.
    pub fn interval(&self) -> Duration {
        self.app().map_or(DEFAULT_INTERVAL, |app| app.interval)
    }

    /// Whether the app is running at `now`. `held_since` is when this manager
    /// started holding the board (it took the status lane); `None` uses the
    /// board's creation.
    pub fn liveness(&self, now: Instant, held_since: Option<Instant>) -> AppLiveness {
        let interval = self.interval();
        let inner = self.lock();
        if inner.apps.values().any(|app| app.fresh(now, interval)) {
            return AppLiveness::Running;
        }
        if !inner.apps.is_empty() {
            return AppLiveness::NotRunning;
        }
        let since = held_since.unwrap_or(inner.created).max(inner.created);
        if now.saturating_duration_since(since) < DEFAULT_INTERVAL * MISSED_INTERVALS {
            AppLiveness::Starting
        } else {
            AppLiveness::NotRunning
        }
    }

    /// The newest evaluation of a target, if any.
    pub fn target(&self, job_id: &str, group: &str, task: &str) -> Option<TargetEntry> {
        self.lock().targets.get(&key(job_id, group, task)).cloned()
    }

    /// Whether an evaluation received at `received` is stale at `now`.
    pub fn is_stale(&self, received: Instant, now: Instant) -> bool {
        now.saturating_duration_since(received) >= self.interval() * MISSED_INTERVALS
    }

    /// Forget everything (tests).
    pub fn clear(&self) {
        let mut inner = self.lock();
        inner.apps.clear();
        inner.targets.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(interval: u32, targets: Vec<ScalingTargetReport>) -> ReportScalingStatusRequest {
        ReportScalingStatusRequest {
            instance: "alloc-1".into(),
            interval_seconds: interval,
            mode: "enforce".into(),
            targets,
            version: "test".into(),
            ..Default::default()
        }
    }

    fn target(decision: &str) -> ScalingTargetReport {
        ScalingTargetReport {
            job_id: "web".into(),
            task_group: "default".into(),
            task_name: "web".into(),
            state: "ok".into(),
            current: 2,
            want: 2,
            evaluated_at_unix_ms: 1,
            summary: "cpu 0.70 vs 0.70".into(),
            decision_json: decision.into(),
            policy_digest: "abc".into(),
            events: Vec::new(),
            policy: String::new(),
        }
    }

    /// NOT RUNNING after three intervals without a report; starting before
    /// the first report while the board is young.
    #[test]
    fn liveness_follows_three_intervals_test() {
        let board = ScalingStatusBoard::default();
        let start = Instant::now();
        assert_eq!(board.liveness(start, None), AppLiveness::Starting);
        assert_eq!(
            board.liveness(start + DEFAULT_INTERVAL * 3, None),
            AppLiveness::NotRunning
        );
        board.record(&report(2, vec![target("{\"x\":1}")]), start, 0);
        assert_eq!(
            board.liveness(start + Duration::from_secs(5), None),
            AppLiveness::Running
        );
        assert_eq!(
            board.liveness(start + Duration::from_secs(6), None),
            AppLiveness::NotRunning
        );
    }

    /// A report that omits an unchanged decision keeps the last one.
    #[test]
    fn unchanged_decisions_are_kept_test() {
        let board = ScalingStatusBoard::default();
        let now = Instant::now();
        board.record(&report(15, vec![target("{\"x\":1}")]), now, 0);
        board.record(&report(15, vec![target("")]), now, 0);
        let entry = board.target("web", "default", "web").expect("entry");
        assert_eq!(entry.decision_json, "{\"x\":1}");
        assert!(!board.is_stale(entry.received, now + Duration::from_secs(44)));
        assert!(board.is_stale(entry.received, now + Duration::from_secs(45)));
    }

    /// Several replicas: running while any reports; each target is the
    /// newest report of it, naming its replica; NOT RUNNING once none has
    /// reported for three intervals.
    #[test]
    fn replicas_report_their_own_targets_test() {
        let board = ScalingStatusBoard::default();
        let start = Instant::now();
        let from = |instance: &str, shards: Vec<u32>, targets| ReportScalingStatusRequest {
            instance: instance.into(),
            interval_seconds: 15,
            mode: "enforce".into(),
            targets,
            version: "test".into(),
            shards,
            shard_count: 64,
        };
        board.record(&from("alloc-1", vec![1, 2], vec![target("{}")]), start, 1);
        let mut other = target("{}");
        other.job_id = "api".into();
        board.record(
            &from("alloc-2", vec![3], vec![other]),
            start + Duration::from_secs(20),
            2,
        );
        let replicas = board.replicas();
        assert_eq!(
            replicas
                .iter()
                .map(|r| r.instance.as_str())
                .collect::<Vec<_>>(),
            vec!["alloc-2", "alloc-1"]
        );
        assert_eq!(replicas[1].shards, vec![1, 2]);
        assert_eq!(
            board.target("web", "default", "web").expect("web").instance,
            "alloc-1"
        );
        assert_eq!(
            board.target("api", "default", "web").expect("api").instance,
            "alloc-2"
        );
        // alloc-1 went quiet: still running on alloc-2's reports.
        let later = start + Duration::from_secs(50);
        assert!(!replicas[1].fresh(later, board.interval()));
        assert_eq!(board.liveness(later, None), AppLiveness::Running);
        // A target moves to the replica that reports it next.
        board.record(
            &from("alloc-2", vec![1, 2, 3], vec![target("{}")]),
            later,
            3,
        );
        assert_eq!(
            board.target("web", "default", "web").expect("web").instance,
            "alloc-2"
        );
        assert_eq!(
            board.liveness(later + Duration::from_secs(45), None),
            AppLiveness::NotRunning
        );
    }
}
