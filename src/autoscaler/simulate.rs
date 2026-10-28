//! The simulator: run a policy over a time series and return the decision
//! sequence, deterministically (plan §6.6 "Dry-run against history"). This
//! is the engine behind `legion scaling simulate`, over the status-lane
//! leader's rollup buffer or a CSV file.
//!
//! It runs the same [`evaluate`] the autoscaler runs, with the same
//! [`PolicyState`], so a simulation can only differ from the live engine
//! in its inputs.
//!
//! # Two modes
//!
//! - **Follow** (the default): the simulated count starts at
//!   [`SimOptions::start_count`] (or the first row's `total`, or the
//!   policy's `min`) and moves with every decision that would change it,
//!   applied or "would" alike, since the point is to see what the policy
//!   would do. New replicas are not ready until `warmup` has passed, unless
//!   a row says how many are ready.
//! - **Replay** (`follow = false`): every row's `total` is the count the
//!   target actually had, and changes between rows are recorded as applied,
//!   so they spend the step budget exactly as they did live. Use it to check
//!   that the engine would have made the decisions it made.
//!
//! # CSV
//!
//! [`read_csv`] reads one header line, then one row per sample:
//!
//! ```text
//! time,cpu,lag,ready,total
//! 2026-03-09T12:00:00Z,0.55,120,4,4
//! 1773057610,0.91,,4,4
//! ```
//!
//! - `time` (required): unix seconds, or RFC 3339. Strictly increasing.
//! - `ready`, `total` (optional): ready replicas and the target's count.
//! - every other column is a check's reading, by check name. An empty cell
//!   (or `-`) is missing data for that sample.
//! - blank lines and lines starting with `#` are skipped.

use std::collections::{BTreeMap, VecDeque};
use std::io::BufRead;

use chrono::DateTime;

use super::defaults::EffectivePolicy;
use super::pipeline::{evaluate, Decision, DecisionKind, Observation, PolicyState, Reading};

/// One sample of a time series.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SimRow {
    /// The sample time (unix ms).
    pub at_ms: i64,
    /// Readings by check name; a check with no entry has no data.
    pub readings: BTreeMap<String, Reading>,
    /// Ready replicas, when the series knows.
    pub ready: Option<u32>,
    /// The target's count, when the series knows (required to replay).
    pub total: Option<u32>,
}

impl SimRow {
    /// A row at `at_ms` with no readings.
    pub fn new(at_ms: i64) -> Self {
        SimRow {
            at_ms,
            ..Default::default()
        }
    }

    /// Add a reading.
    pub fn reading(mut self, check: &str, value: f64) -> Self {
        self.readings
            .insert(check.to_string(), Reading::Value(value));
        self
    }
}

/// How to run a simulation.
#[derive(Debug, Clone, PartialEq)]
pub struct SimOptions {
    /// The starting count in follow mode. Default: the first row's
    /// `total`, else the policy's `min`.
    pub start_count: Option<u32>,
    /// Follow the policy's own decisions (true, the default) or replay the
    /// rows' `total`.
    pub follow: bool,
    /// When the simulated engine started, for the cold-start hold. Default:
    /// the first row's time, like a freshly started engine. Pass a time at
    /// least one `down.stabilize` earlier to simulate a warm engine.
    pub started_at_ms: Option<i64>,
}

impl Default for SimOptions {
    fn default() -> Self {
        SimOptions {
            start_count: None,
            follow: true,
            started_at_ms: None,
        }
    }
}

/// What a step of a model-driven simulation sees of the simulated target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimView {
    /// The sample time (unix ms).
    pub now_ms: i64,
    /// The simulated count.
    pub count: u32,
    /// Replicas past warmup.
    pub ready: u32,
    /// Replicas still warming.
    pub warming: u32,
}

/// A simulated target driven by one policy: the count, the replicas still
/// warming, and the engine's [`PolicyState`].
#[derive(Debug, Clone)]
pub struct Simulation<'a> {
    policy: &'a EffectivePolicy,
    state: PolicyState,
    count: u32,
    /// Replicas added by scale-outs that are not ready yet: (ready at, n).
    warming: VecDeque<(i64, u32)>,
}

impl<'a> Simulation<'a> {
    /// A target at `start_count`, with an engine that started at
    /// `started_at_ms`.
    pub fn new(policy: &'a EffectivePolicy, start_count: u32, started_at_ms: i64) -> Self {
        Simulation {
            policy,
            state: PolicyState::new(started_at_ms),
            count: start_count,
            warming: VecDeque::new(),
        }
    }

    /// The simulated count.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// The engine's state.
    pub fn state(&self) -> &PolicyState {
        &self.state
    }

    /// What the target looks like at `now_ms`.
    pub fn view(&mut self, now_ms: i64) -> SimView {
        while self
            .warming
            .front()
            .is_some_and(|(ready_at, _)| *ready_at <= now_ms)
        {
            self.warming.pop_front();
        }
        let warming: u32 = self.warming.iter().map(|(_, n)| *n).sum();
        let warming = warming.min(self.count);
        SimView {
            now_ms,
            count: self.count,
            ready: self.count - warming,
            warming,
        }
    }

    /// Evaluate one sample and, when the decision would change the count,
    /// move the simulated target there.
    pub fn step(
        &mut self,
        now_ms: i64,
        readings: BTreeMap<String, Reading>,
        ready: Option<u32>,
    ) -> Decision {
        let view = self.view(now_ms);
        let ready = ready.map(|r| r.min(view.count)).unwrap_or(view.ready);
        let obs = Observation {
            current: view.count,
            ready,
            warming: view.count - ready,
            readings,
            ..Default::default()
        };
        let decision = evaluate(self.policy, &mut self.state, &obs, now_ms);
        if matches!(decision.kind, DecisionKind::Applied | DecisionKind::Would) {
            self.apply(now_ms, decision.to);
        }
        decision
    }

    fn apply(&mut self, now_ms: i64, to: u32) {
        let from = self.count;
        self.state.record_applied(now_ms, from, to);
        if to > from {
            let ready_at = now_ms + i64::try_from(self.policy.warmup.as_millis()).unwrap_or(0);
            self.warming.push_back((ready_at, to - from));
        } else {
            // Scale-in removes the newest replicas first.
            let mut remove = from - to;
            while remove > 0 {
                let Some(last) = self.warming.back_mut() else {
                    break;
                };
                let take = last.1.min(remove);
                last.1 -= take;
                remove -= take;
                if last.1 == 0 {
                    self.warming.pop_back();
                }
            }
        }
        self.count = to;
    }

    /// Replay mode: the target is at `total` because it was, not because
    /// the policy said so. A change since the last sample is recorded as
    /// applied, so it spends the step budget.
    fn observe_total(&mut self, now_ms: i64, total: u32) {
        if total != self.count {
            self.state.record_applied(now_ms, self.count, total);
            self.count = total;
            self.warming.clear();
        }
    }
}

fn check_order(rows: &[SimRow]) -> Result<(), String> {
    for pair in rows.windows(2) {
        if pair[1].at_ms <= pair[0].at_ms {
            return Err(format!(
                "samples must be in increasing time order: {} comes after {}",
                pair[1].at_ms, pair[0].at_ms
            ));
        }
    }
    Ok(())
}

/// Run `policy` over `rows` and return one decision per row.
/// Deterministic: the same policy, rows and options always produce the same
/// decisions.
pub fn simulate(
    policy: &EffectivePolicy,
    rows: &[SimRow],
    options: &SimOptions,
) -> Result<Vec<Decision>, String> {
    check_order(rows)?;
    let Some(first) = rows.first() else {
        return Ok(Vec::new());
    };
    let start = if options.follow {
        options.start_count.or(first.total).unwrap_or(policy.min)
    } else {
        first.total.ok_or_else(|| {
            format!(
                "replay needs every sample's total; the sample at {} has none",
                first.at_ms
            )
        })?
    };
    let started = options.started_at_ms.unwrap_or(first.at_ms);
    let mut simulation = Simulation::new(policy, start, started);
    let mut decisions = Vec::with_capacity(rows.len());
    for row in rows {
        if !options.follow {
            let total = row.total.ok_or_else(|| {
                format!(
                    "replay needs every sample's total; the sample at {} has none",
                    row.at_ms
                )
            })?;
            simulation.observe_total(row.at_ms, total);
            let ready = row.ready.unwrap_or(total);
            let obs = Observation {
                current: total,
                ready: ready.min(total),
                warming: total - ready.min(total),
                readings: row.readings.clone(),
                ..Default::default()
            };
            decisions.push(evaluate(policy, &mut simulation.state, &obs, row.at_ms));
        } else {
            decisions.push(simulation.step(row.at_ms, row.readings.clone(), row.ready));
        }
    }
    Ok(decisions)
}

/// Run `policy` against a model: at each time in `times`, `readings` is
/// given the simulated target and returns that sample's readings. For
/// synthetic load (a ramp, a spike, a sawtooth) where the reading depends
/// on the count the policy chose, which a fixed series cannot express.
pub fn simulate_with<F>(
    policy: &EffectivePolicy,
    start_count: u32,
    started_at_ms: i64,
    times: impl IntoIterator<Item = i64>,
    mut readings: F,
) -> Vec<Decision>
where
    F: FnMut(&SimView) -> BTreeMap<String, Reading>,
{
    let mut simulation = Simulation::new(policy, start_count, started_at_ms);
    let mut decisions = Vec::new();
    for now_ms in times {
        let view = simulation.view(now_ms);
        let sample = readings(&view);
        decisions.push(simulation.step(now_ms, sample, None));
    }
    decisions
}

fn parse_time(text: &str) -> Result<i64, String> {
    if let Ok(seconds) = text.parse::<i64>() {
        return Ok(seconds.saturating_mul(1000));
    }
    DateTime::parse_from_rfc3339(text)
        .map(|at| at.timestamp_millis())
        .map_err(|_| format!("'{text}' is not a time (unix seconds or RFC 3339)"))
}

/// Read a series from CSV (see the module docs for the format).
pub fn read_csv<R: BufRead>(reader: R) -> Result<Vec<SimRow>, String> {
    let mut header: Option<Vec<String>> = None;
    let mut rows = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let number = index + 1;
        let line = line.map_err(|err| format!("line {number}: {err}"))?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let cells: Vec<&str> = line.split(',').map(str::trim).collect();
        let Some(columns) = &header else {
            if !cells.contains(&"time") {
                return Err(format!(
                    "line {number}: the header needs a 'time' column (then ready, total, and \
                     one column per check)"
                ));
            }
            let mut seen = std::collections::BTreeSet::new();
            for cell in &cells {
                if cell.is_empty() || !seen.insert(*cell) {
                    return Err(format!(
                        "line {number}: header column '{cell}' is empty or repeated"
                    ));
                }
            }
            header = Some(cells.iter().map(|c| c.to_string()).collect());
            continue;
        };
        if cells.len() != columns.len() {
            return Err(format!(
                "line {number}: {} cells, but the header has {} columns",
                cells.len(),
                columns.len()
            ));
        }
        let mut row = SimRow::default();
        for (column, cell) in columns.iter().zip(cells) {
            let at = format!("line {number}, column {column}");
            match column.as_str() {
                "time" => row.at_ms = parse_time(cell).map_err(|err| format!("{at}: {err}"))?,
                "ready" | "total" => {
                    let value = if cell.is_empty() || cell == "-" {
                        None
                    } else {
                        Some(
                            cell.parse::<u32>()
                                .map_err(|_| format!("{at}: '{cell}' is not a replica count"))?,
                        )
                    };
                    if column == "ready" {
                        row.ready = value;
                    } else {
                        row.total = value;
                    }
                }
                check => {
                    let reading = if cell.is_empty() || cell == "-" {
                        Reading::Missing("no data in the series".to_string())
                    } else {
                        let value: f64 = cell
                            .parse()
                            .map_err(|_| format!("{at}: '{cell}' is not a number"))?;
                        if !value.is_finite() {
                            return Err(format!("{at}: '{cell}' is not a finite number"));
                        }
                        Reading::Value(value)
                    };
                    row.readings.insert(check.to_string(), reading);
                }
            }
        }
        rows.push(row);
    }
    if header.is_none() {
        return Err("the series is empty: it needs a header line with a 'time' column".to_string());
    }
    check_order(&rows)?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autoscaler::defaults::{resolve, DefaultsLayers};
    use crate::autoscaler::pipeline::{Flag, Stage};
    use crate::autoscaler::spec::AutoscaleSpec;
    use serde_json::{json, Value};

    /// 2026-01-01T00:00:00Z.
    const T0: i64 = 1_767_225_600_000;

    fn at(secs: i64) -> i64 {
        T0 + secs * 1000
    }

    fn policy(task: Value, count: u32) -> EffectivePolicy {
        let spec: AutoscaleSpec = serde_json::from_value(task).unwrap();
        resolve(&spec, count, &DefaultsLayers::default(), "web/default/web")
            .unwrap()
            .unwrap()
            .policy
    }

    fn changes(decisions: &[Decision]) -> Vec<(i64, u32, u32)> {
        decisions
            .iter()
            .filter(|d| d.kind != DecisionKind::Held)
            .map(|d| ((d.at_ms - T0) / 1000, d.from, d.to))
            .collect()
    }

    /// Plan §3, S1: `autoscale = true` on a task with count 2 and cpu 500.
    /// Load ramps to 3500 MHz at 1:00 and falls back to 500 MHz at 5:00.
    /// It scales out within a minute of the spike, capped by the step limit
    /// (+4, then to the ceiling of 8), holds at the ceiling, and after five
    /// quiet minutes scales in at one replica per minute down to the floor.
    #[test]
    fn golden_s1_one_word_test() {
        let policy = policy(json!(true), 2);
        let demand_mhz = |secs: i64| match secs {
            s if s < 60 => 500.0,
            s if s < 300 => 3500.0,
            _ => 500.0,
        };
        let decisions = simulate_with(&policy, 2, at(0), (0..=1200).step_by(10).map(at), |view| {
            let secs = (view.now_ms - T0) / 1000;
            let utilization = demand_mhz(secs) / (f64::from(view.count) * 500.0);
            BTreeMap::from([("cpu".to_string(), Reading::Value(utilization))])
        });
        assert_eq!(
            changes(&decisions),
            vec![
                (60, 2, 6),
                (120, 6, 8),
                (590, 8, 7),
                (650, 7, 6),
                (710, 6, 5),
                (770, 5, 4),
                (830, 4, 3),
                (890, 3, 2),
            ]
        );
        let spike = decisions.iter().find(|d| d.at_ms == at(60)).unwrap();
        assert_eq!(
            spike.summary,
            "cpu 3.50 vs 0.70 (2 ready); rate-limited from 10"
        );
        let ceiling = decisions.iter().find(|d| d.at_ms == at(120)).unwrap();
        assert!(
            ceiling.summary.contains("clamped to max 8"),
            "{}",
            ceiling.summary
        );
        assert_eq!(
            ceiling.flags,
            vec![Flag::Limited {
                reason: "at max 8 (raise autoscale.max)".to_string()
            }]
        );
        // The first quiet minutes are held by the 5m down window.
        let quiet = decisions.iter().find(|d| d.at_ms == at(400)).unwrap();
        assert_eq!(quiet.kind, DecisionKind::Held);
        assert!(
            quiet
                .stage(Stage::Stabilize)
                .unwrap()
                .note
                .contains("down: 5m max is 10"),
            "{quiet:#?}"
        );
        for decision in &decisions {
            assert!((2..=8).contains(&decision.to), "{decision:#?}");
            assert!(decision.kind != DecisionKind::Would);
        }
    }

    #[test]
    fn simulate_is_deterministic_over_csv_test() {
        let csv = "\
# a spike, then a dropped source, then quiet
time,cpu,ready,total
1767225600,0.5,2,2
1767225610,2.0,2,2
1767225620,1.4,,
1767225630,,,
1767225640,-,,
2026-01-01T00:01:00Z,0.3,,
2026-01-01T00:07:00Z,0.2,,
2026-01-01T00:08:00Z,0.2,,
";
        let rows = read_csv(csv.as_bytes()).unwrap();
        assert_eq!(rows.len(), 8);
        assert_eq!(rows[0].at_ms, T0);
        assert_eq!(rows[5].at_ms, at(60));
        assert_eq!(rows[0].ready, Some(2));
        assert_eq!(rows[2].total, None);
        assert!(matches!(rows[3].readings["cpu"], Reading::Missing(_)));
        let policy = policy(json!({ "max": 10 }), 2);
        let options = SimOptions::default();
        let first = simulate(&policy, &rows, &options).unwrap();
        let second = simulate(&policy, &rows, &options).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            serde_json::to_string(&first).unwrap(),
            serde_json::to_string(&second).unwrap()
        );
        assert_eq!(first.len(), rows.len());
        // 2 → 6 at the spike; no scale-in while the source is missing or in
        // the cold-start window; then the scale-in rate.
        assert_eq!(changes(&first)[0], (10, 2, 6));
        for decision in &first[3..5] {
            assert_eq!(decision.kind, DecisionKind::Held, "{decision:#?}");
        }
        assert!(first[5]
            .stage(Stage::Hold)
            .unwrap()
            .note
            .contains("collecting 5m"));
        assert_eq!(changes(&first)[1..], [(420, 6, 5), (480, 5, 4)]);
    }

    #[test]
    fn replay_spends_the_step_budget_of_recorded_changes_test() {
        let policy = policy(json!({ "max": 20 }), 2);
        let rows = vec![
            SimRow {
                total: Some(2),
                ..SimRow::new(at(0)).reading("cpu", 0.7)
            },
            SimRow {
                total: Some(6),
                ..SimRow::new(at(10)).reading("cpu", 2.0)
            },
        ];
        let options = SimOptions {
            follow: false,
            ..Default::default()
        };
        let decisions = simulate(&policy, &rows, &options).unwrap();
        // The recorded 2 → 6 used the +4 budget, so the second sample holds.
        assert_eq!(decisions[1].from, 6);
        assert_eq!(decisions[1].kind, DecisionKind::Held, "{:#?}", decisions[1]);
        assert!(decisions[1]
            .stage(Stage::Step)
            .unwrap()
            .note
            .contains("+4 used"));
        let missing_total = vec![SimRow::new(at(0)).reading("cpu", 0.7)];
        let err = simulate(&policy, &missing_total, &options).unwrap_err();
        assert!(err.contains("replay needs every sample's total"), "{err}");
    }

    #[test]
    fn sawtooth_engages_the_flap_guard_and_holds_scale_in_test() {
        let policy = policy(
            json!({ "max": 20, "down": { "stabilize": "30s", "step": { "percent": 100 } } }),
            2,
        );
        let decisions = simulate_with(
            &policy,
            2,
            at(-3600),
            (0..=1800).step_by(10).map(at),
            |view| {
                let secs = (view.now_ms - T0) / 1000;
                let demand = if (secs / 60) % 2 == 0 { 3000.0 } else { 500.0 };
                let utilization = demand / (f64::from(view.count.max(1)) * 500.0);
                BTreeMap::from([("cpu".to_string(), Reading::Value(utilization))])
            },
        );
        let holds: Vec<(i64, i64)> = decisions
            .iter()
            .flat_map(|d| {
                d.flags.iter().filter_map(move |flag| match flag {
                    Flag::Flapping { until_ms, .. } => Some((d.at_ms, *until_ms)),
                    _ => None,
                })
            })
            .collect();
        assert!(!holds.is_empty(), "the sawtooth should trip the flap guard");
        for (start, until) in &holds {
            assert_eq!(*until - *start, 60_000, "2 × down.stabilize");
            for decision in decisions
                .iter()
                .filter(|d| d.at_ms >= *start && d.at_ms < *until)
            {
                assert!(
                    decision.to >= decision.from,
                    "scale-in during a flap hold: {decision:#?}"
                );
            }
        }
        // Each episode is flagged once: a flag is never repeated inside the
        // hold it started.
        for pair in holds.windows(2) {
            assert!(pair[1].0 >= pair[0].1, "{holds:?}");
        }
    }

    #[test]
    fn csv_errors_test() {
        let cases = [
            ("cpu,total\n0.5,2\n", "needs a 'time' column"),
            (
                "time,cpu\n0,abc\n",
                "line 2, column cpu: 'abc' is not a number",
            ),
            (
                "time,cpu\n0,0.5,9\n",
                "3 cells, but the header has 2 columns",
            ),
            ("time,cpu\nyesterday,0.5\n", "is not a time"),
            ("time,cpu,ready\n0,0.5,many\n", "is not a replica count"),
            ("time,cpu\n10,0.5\n5,0.5\n", "increasing time order"),
            ("time,cpu,cpu\n", "repeated"),
            ("", "the series is empty"),
        ];
        for (csv, expected) in cases {
            let err = read_csv(csv.as_bytes()).unwrap_err();
            assert!(err.contains(expected), "{csv:?}: {err}");
        }
    }
}
