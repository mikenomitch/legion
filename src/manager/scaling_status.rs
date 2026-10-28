//! What the autoscaler app reports (`ReportScalingStatus`; docs/autoscaling.md,
//! "Knowing whether the autoscaler is running").
//!
//! The app sends one report per evaluation tick: its interval, its mode, and
//! the last evaluation of every target. The manager that leads the status
//! lane holds the newest report per target in memory, like usage rollups:
//! nothing is replicated, a failover starts empty, and the next report (one
//! interval later) refills it. `GetScalingStatus` reads it back, and says
//! the app is NOT RUNNING once three of its intervals pass with no report,
//! so `legion scaling status` works, and is honest, with the app down.

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

/// Targets not reported for this long are forgotten (a removed policy).
const TARGET_RETENTION: Duration = Duration::from_secs(3600);

/// The newest report from the app as a whole.
#[derive(Debug, Clone)]
pub struct AppReport {
    pub instance: String,
    pub interval: Duration,
    pub mode: String,
    pub version: String,
    pub received: Instant,
    pub received_unix_ms: i64,
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
}

/// Whether the app is running, as the reports show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppLiveness {
    /// A report arrived within [`MISSED_INTERVALS`] intervals.
    Running,
    /// None did. `since` is the last report, if any.
    NotRunning,
    /// No report yet, and this manager has not held the board for
    /// [`MISSED_INTERVALS`] default intervals: too early to tell.
    Starting,
}

#[derive(Debug)]
struct Inner {
    created: Instant,
    app: Option<AppReport>,
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
                app: None,
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
        inner.app = Some(AppReport {
            instance: report.instance.clone(),
            interval,
            mode: report.mode.clone(),
            version: report.version.clone(),
            received: now,
            received_unix_ms: now_unix_ms,
        });
        for target in &report.targets {
            Self::record_target(&mut inner, target, now);
        }
        inner
            .targets
            .retain(|_, entry| now.saturating_duration_since(entry.received) < TARGET_RETENTION);
        report.targets.len() as u32
    }

    fn record_target(inner: &mut Inner, target: &ScalingTargetReport, now: Instant) {
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
            },
        );
    }

    /// The newest app report, if any.
    pub fn app(&self) -> Option<AppReport> {
        self.lock().app.clone()
    }

    /// The app's interval: reported, or the default.
    pub fn interval(&self) -> Duration {
        self.lock()
            .app
            .as_ref()
            .map_or(DEFAULT_INTERVAL, |app| app.interval)
    }

    /// Whether the app is running at `now`. `held_since` is when this manager
    /// started holding the board (it took the status lane); `None` uses the
    /// board's creation.
    pub fn liveness(&self, now: Instant, held_since: Option<Instant>) -> AppLiveness {
        let inner = self.lock();
        match &inner.app {
            Some(app) => {
                if now.saturating_duration_since(app.received) < app.interval * MISSED_INTERVALS {
                    AppLiveness::Running
                } else {
                    AppLiveness::NotRunning
                }
            }
            None => {
                let since = held_since.unwrap_or(inner.created).max(inner.created);
                if now.saturating_duration_since(since) < DEFAULT_INTERVAL * MISSED_INTERVALS {
                    AppLiveness::Starting
                } else {
                    AppLiveness::NotRunning
                }
            }
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
        inner.app = None;
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
}
