//! Built-in strategies: pure functions from one check's reading to a desired
//! replica count (plan §6.4).
//!
//! | Strategy | Settings | Desired count |
//! | --- | --- | --- |
//! | `target-value` | `target` | the HPA formula, `ceil(current × reading / target)`, with a tolerance band and the warmup rule |
//! | `per-replica` | `value` | `ceil(reading / value)` |
//! | `threshold` | `steps = [{ above, add }]` | `current + add` of the highest step the reading is above |
//! | `pass-through` | | `ceil(reading)`: the reading *is* the count |
//! | `fixed` | `value` | `value`, whatever the reading |
//!
//! Any other name is a **strategy plugin** installed in the autoscaler's app
//! config ([`super::plugins`]): `{ name = "calendar", replicas = 6 }` is
//! [`StrategyConfig::Plugin`], whose settings are the plugin's to check. It
//! does I/O, so the pipeline does not call it: the autoscaler asks the
//! plugin first and passes its answer in the
//! [`Observation`](super::pipeline::Observation).
//!
//! A strategy sees one check; merging checks, stabilization, step limits
//! and bounds all happen later in the [pipeline](super::pipeline).
//!
//! # The warmup rule
//!
//! For a utilization reading (an average over *ready* replicas, like the
//! `cpu` shorthand), replicas that are not ready yet (warming up, starting,
//! or unplaced) have no usage to average. `target-value` follows the HPA:
//! when deciding to scale out it counts them as 0% busy, and when deciding
//! to scale in it counts them as exactly at target. Either way they damp the
//! move rather than cause one, and if counting them reverses the direction
//! or lands inside the tolerance band, the strategy holds.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::spec::StrategySpec;

/// The names of the built-in strategies, in the order docs list them.
pub const BUILT_IN_STRATEGIES: [&str; 5] = [
    "target-value",
    "per-replica",
    "threshold",
    "pass-through",
    "fixed",
];

/// What a strategy is told about one check, at one evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyInput {
    /// The target's current desired count.
    pub current: u32,
    /// The check's reading. Ignored by strategies that read nothing.
    pub reading: f64,
    /// Ready replicas (healthy and past warmup).
    pub ready: u32,
    /// The policy's tolerance band, as a fraction of the target.
    pub tolerance: f64,
    /// Whether `reading` is an average over ready replicas (a utilization),
    /// which is what the warmup rule applies to.
    pub per_ready_replica: bool,
}

/// One strategy's answer.
#[derive(Debug, Clone, PartialEq)]
pub struct Recommendation {
    /// The desired replica count.
    pub desired: u32,
    /// What the strategy saw, for notes and the decision summary
    /// (`"0.91 vs 0.70 (4 ready)"`).
    pub detail: String,
    /// The reading was inside the tolerance band (or the warmup rule
    /// cancelled the move), so the strategy held at the current count.
    pub within_tolerance: bool,
}

/// A pure strategy. Implementations must be deterministic: the same input
/// always yields the same recommendation.
pub trait Strategy {
    /// The strategy's name as policies write it.
    fn name(&self) -> &'static str;

    /// Whether the strategy needs a reading. `fixed` does not, so a check
    /// using it never has missing data.
    fn needs_reading(&self) -> bool {
        true
    }

    /// Turn one reading into a desired count.
    fn recommend(&self, input: &StrategyInput) -> Recommendation;
}

/// A strategy with its settings, as the effective policy carries it.
/// Serializes as the record it was written as:
/// `{ "name": "per-replica", "value": 500 }`.
#[derive(Debug, Clone, PartialEq)]
pub enum StrategyConfig {
    /// See [`TargetValue`].
    TargetValue(TargetValue),
    /// See [`PerReplica`].
    PerReplica(PerReplica),
    /// See [`Threshold`].
    Threshold(Threshold),
    /// See [`PassThrough`].
    PassThrough(PassThrough),
    /// See [`Fixed`].
    Fixed(Fixed),
    /// A strategy plugin, by the name it is installed under.
    Plugin(PluginStrategy),
}

/// The built-ins' wire form: tagged by `name`.
#[derive(Serialize, Deserialize)]
#[serde(tag = "name", rename_all = "kebab-case")]
enum BuiltinRepr {
    TargetValue(TargetValue),
    PerReplica(PerReplica),
    Threshold(Threshold),
    PassThrough(PassThrough),
    Fixed(Fixed),
}

impl Serialize for StrategyConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let repr = match self {
            StrategyConfig::TargetValue(s) => BuiltinRepr::TargetValue(s.clone()),
            StrategyConfig::PerReplica(s) => BuiltinRepr::PerReplica(s.clone()),
            StrategyConfig::Threshold(s) => BuiltinRepr::Threshold(s.clone()),
            StrategyConfig::PassThrough(s) => BuiltinRepr::PassThrough(s.clone()),
            StrategyConfig::Fixed(s) => BuiltinRepr::Fixed(s.clone()),
            StrategyConfig::Plugin(plugin) => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(plugin.config.len() + 1))?;
                map.serialize_entry("name", &plugin.name)?;
                for (key, value) in &plugin.config {
                    map.serialize_entry(key, value)?;
                }
                return map.end();
            }
        };
        repr.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StrategyConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = Value::deserialize(deserializer)?;
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::custom("a strategy needs a name"))?
            .to_string();
        if BUILT_IN_STRATEGIES.contains(&name.as_str()) {
            let repr: BuiltinRepr = serde_json::from_value(value).map_err(D::Error::custom)?;
            return Ok(match repr {
                BuiltinRepr::TargetValue(s) => StrategyConfig::TargetValue(s),
                BuiltinRepr::PerReplica(s) => StrategyConfig::PerReplica(s),
                BuiltinRepr::Threshold(s) => StrategyConfig::Threshold(s),
                BuiltinRepr::PassThrough(s) => StrategyConfig::PassThrough(s),
                BuiltinRepr::Fixed(s) => StrategyConfig::Fixed(s),
            });
        }
        let mut config = BTreeMap::new();
        if let Value::Object(object) = value {
            for (key, value) in object {
                if key != "name" {
                    config.insert(key, value);
                }
            }
        }
        Ok(StrategyConfig::Plugin(PluginStrategy { name, config }))
    }
}

impl StrategyConfig {
    /// Parse a written strategy record into a typed built-in, with errors
    /// that name the strategy and the setting.
    ///
    /// A name that is not built in is a strategy plugin's: it must be a
    /// valid plugin name, and whether one is installed (and open to the
    /// policy's namespace) is checked where the plugins are known, at
    /// submit and apply.
    pub fn from_spec(spec: &StrategySpec) -> Result<StrategyConfig, String> {
        let name = spec.name.trim();
        if !BUILT_IN_STRATEGIES.contains(&name) {
            if !super::plugins::valid_name(name) {
                return Err(format!(
                    "unknown strategy '{name}'. Hint: the built-in strategies are {}; any other \
                     name is a strategy plugin installed in the autoscaler's app config, \
                     written in lowercase letters, digits and `-`",
                    BUILT_IN_STRATEGIES.join(", ")
                ));
            }
            return Ok(StrategyConfig::Plugin(PluginStrategy {
                name: name.to_string(),
                config: spec.config.clone(),
            }));
        }
        let mut object = serde_json::Map::new();
        for (key, value) in &spec.config {
            object.insert(key.clone(), value.clone());
        }
        object.insert("name".to_string(), Value::String(name.to_string()));
        let config: StrategyConfig = serde_json::from_value(Value::Object(object))
            .map_err(|err| format!("strategy '{name}': {err}"))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        match self {
            StrategyConfig::TargetValue(s) if !(s.target.is_finite() && s.target > 0.0) => {
                Err(format!(
                    "strategy 'target-value': target = {} must be greater than 0",
                    s.target
                ))
            }
            StrategyConfig::PerReplica(s) if !(s.value.is_finite() && s.value > 0.0) => {
                Err(format!(
                    "strategy 'per-replica': value = {} must be greater than 0",
                    s.value
                ))
            }
            StrategyConfig::Threshold(s) if s.steps.is_empty() => {
                Err("strategy 'threshold': steps must list at least one { above, add }".to_string())
            }
            StrategyConfig::Threshold(s) if s.steps.iter().any(|step| !step.above.is_finite()) => {
                Err("strategy 'threshold': every step's above must be a number".to_string())
            }
            _ => Ok(()),
        }
    }

    /// The strategy behind this config.
    pub fn strategy(&self) -> &dyn Strategy {
        match self {
            StrategyConfig::TargetValue(s) => s,
            StrategyConfig::PerReplica(s) => s,
            StrategyConfig::Threshold(s) => s,
            StrategyConfig::PassThrough(s) => s,
            StrategyConfig::Fixed(s) => s,
            StrategyConfig::Plugin(s) => s,
        }
    }

    /// The plugin, when this is a strategy plugin.
    pub fn plugin(&self) -> Option<&PluginStrategy> {
        match self {
            StrategyConfig::Plugin(plugin) => Some(plugin),
            _ => None,
        }
    }

    /// Whether this strategy reads a value.
    pub fn needs_reading(&self) -> bool {
        self.strategy().needs_reading()
    }

    /// The `target-value` target, if this is one (used to describe the
    /// `cpu`/`memory` shorthands).
    pub fn target(&self) -> Option<f64> {
        match self {
            StrategyConfig::TargetValue(s) => Some(s.target),
            _ => None,
        }
    }
}

impl fmt::Display for StrategyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StrategyConfig::TargetValue(s) => write!(f, "target-value {}", fmt_num(s.target)),
            StrategyConfig::PerReplica(s) => write!(f, "per-replica {}", fmt_num(s.value)),
            StrategyConfig::Threshold(s) => write!(f, "threshold ({} steps)", s.steps.len()),
            StrategyConfig::PassThrough(_) => f.write_str("pass-through"),
            StrategyConfig::Fixed(s) => write!(f, "fixed {}", s.value),
            StrategyConfig::Plugin(s) => write!(f, "plugin {}", s.name),
        }
    }
}

/// `target-value`: hold the reading at `target` (the HPA formula). The
/// `cpu` and `memory` shorthands are this strategy on a utilization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetValue {
    /// The value to hold the reading at (a fraction of the reservation for
    /// a utilization).
    pub target: f64,
}

/// `per-replica`: one replica per `value` of the reading, so a queue depth
/// of 2300 with `value = 500` wants 5.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerReplica {
    /// How much of the reading one replica handles.
    pub value: f64,
}

/// `threshold`: step rules. The highest step whose `above` the reading
/// exceeds adds its `add` (which may be negative) to the current count;
/// below every step, the strategy holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Threshold {
    /// The steps, in any order.
    pub steps: Vec<ThresholdStep>,
}

/// One `threshold` step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdStep {
    /// The step applies when the reading is strictly above this.
    pub above: f64,
    /// Replicas to add to the current count (negative removes).
    pub add: i64,
}

/// `pass-through`: the reading is the desired count, rounded up. For
/// signals where an external system already decided.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassThrough {}

/// A strategy plugin: its installed name and the settings the policy wrote
/// beside it. The plugin checks them against its `policyConfigSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct PluginStrategy {
    /// The name the plugin is installed under.
    pub name: String,
    /// Every field of the strategy record but `name`.
    pub config: BTreeMap<String, Value>,
}

impl PluginStrategy {
    /// The settings as one JSON record, as `strategy/recommend` sends them.
    pub fn config_value(&self) -> Value {
        Value::Object(
            self.config
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )
    }
}

/// `fixed`: always `value`, whatever the reading. Reads nothing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixed {
    /// The desired count.
    pub value: u32,
}

/// Round a fractional replica count up to a whole one, ignoring float
/// noise just above an integer (`4.0000000001` is 4), and clamping
/// non-finite or negative values to 0.
pub fn to_count(value: f64) -> u32 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    let rounded = (value - 1e-9).ceil();
    if rounded >= f64::from(u32::MAX) {
        u32::MAX
    } else {
        rounded.max(0.0) as u32
    }
}

/// Format a reading compactly: whole numbers without decimals, others with
/// up to two.
pub fn fmt_num(value: f64) -> String {
    if value.is_finite() && (value - value.round()).abs() < 1e-9 && value.abs() < 1e15 {
        return format!("{}", value.round() as i64);
    }
    let text = format!("{value:.2}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn hold(current: u32, detail: String) -> Recommendation {
    Recommendation {
        desired: current,
        detail,
        within_tolerance: true,
    }
}

impl Strategy for TargetValue {
    fn name(&self) -> &'static str {
        "target-value"
    }

    fn recommend(&self, input: &StrategyInput) -> Recommendation {
        let target = self.target;
        let reading = input.reading.max(0.0);
        let tolerance = input.tolerance;
        let ratio = reading / target;
        let mut detail = format!("{reading:.2} vs {target:.2}");

        if !input.per_ready_replica {
            if (ratio - 1.0).abs() <= tolerance {
                return hold(input.current, format!("{detail}, within tolerance"));
            }
            return Recommendation {
                desired: to_count(f64::from(input.current) * ratio),
                detail,
                within_tolerance: false,
            };
        }

        // A utilization: averaged over ready replicas only.
        let total = input.current.max(input.ready);
        let not_ready = total - input.ready;
        if not_ready > 0 {
            detail.push_str(&format!(" ({} ready, {not_ready} not ready)", input.ready));
        } else {
            detail.push_str(&format!(" ({} ready)", input.ready));
        }
        if input.ready == 0 {
            return hold(input.current, format!("{detail}, nothing ready to measure"));
        }
        if (ratio - 1.0).abs() <= tolerance {
            return hold(input.current, format!("{detail}, within tolerance"));
        }
        let ready = f64::from(input.ready);
        let total_f = f64::from(total);
        if not_ready == 0 {
            return Recommendation {
                desired: to_count(ratio * ready),
                detail,
                within_tolerance: false,
            };
        }
        // The warmup rule (HPA): not-ready replicas count as 0% busy when
        // scaling out and as exactly at target when scaling in.
        let adjusted = if ratio > 1.0 {
            (reading * ready) / total_f / target
        } else {
            (reading * ready + target * f64::from(not_ready)) / total_f / target
        };
        let reversed = (ratio > 1.0) != (adjusted > 1.0);
        if reversed || (adjusted - 1.0).abs() <= tolerance {
            return hold(
                input.current,
                format!("{detail}, held: not-ready replicas cancel the move"),
            );
        }
        Recommendation {
            desired: to_count(adjusted * total_f),
            detail,
            within_tolerance: false,
        }
    }
}

impl Strategy for PerReplica {
    fn name(&self) -> &'static str {
        "per-replica"
    }

    fn recommend(&self, input: &StrategyInput) -> Recommendation {
        Recommendation {
            desired: to_count(input.reading / self.value),
            detail: format!(
                "{} / {} per replica",
                fmt_num(input.reading),
                fmt_num(self.value)
            ),
            within_tolerance: false,
        }
    }
}

impl Strategy for Threshold {
    fn name(&self) -> &'static str {
        "threshold"
    }

    fn recommend(&self, input: &StrategyInput) -> Recommendation {
        let step = self
            .steps
            .iter()
            .filter(|step| input.reading > step.above)
            .max_by(|a, b| a.above.total_cmp(&b.above));
        match step {
            Some(step) => {
                let desired = (i64::from(input.current) + step.add).clamp(0, i64::from(u32::MAX));
                Recommendation {
                    desired: desired as u32,
                    detail: format!(
                        "{} above {}: {:+}",
                        fmt_num(input.reading),
                        fmt_num(step.above),
                        step.add
                    ),
                    within_tolerance: false,
                }
            }
            None => Recommendation {
                desired: input.current,
                detail: format!("{} is below every threshold", fmt_num(input.reading)),
                within_tolerance: false,
            },
        }
    }
}

impl Strategy for PassThrough {
    fn name(&self) -> &'static str {
        "pass-through"
    }

    fn recommend(&self, input: &StrategyInput) -> Recommendation {
        Recommendation {
            desired: to_count(input.reading),
            detail: format!("{} (pass-through)", fmt_num(input.reading)),
            within_tolerance: false,
        }
    }
}

/// The pipeline never asks a plugin itself (it is pure): it reads the
/// plugin's answer from the observation. This only says so, for a caller
/// that recommends without one.
impl Strategy for PluginStrategy {
    fn name(&self) -> &'static str {
        "plugin"
    }

    /// A plugin reads the check's reading when the check has a query, and
    /// is asked without one otherwise.
    fn needs_reading(&self) -> bool {
        false
    }

    fn recommend(&self, input: &StrategyInput) -> Recommendation {
        hold(
            input.current,
            format!("strategy plugin {} was not asked", self.name),
        )
    }
}

impl Strategy for Fixed {
    fn name(&self) -> &'static str {
        "fixed"
    }

    fn needs_reading(&self) -> bool {
        false
    }

    fn recommend(&self, _input: &StrategyInput) -> Recommendation {
        Recommendation {
            desired: self.value,
            detail: format!("fixed at {}", self.value),
            within_tolerance: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn input(current: u32, reading: f64, ready: u32, per_ready: bool) -> StrategyInput {
        StrategyInput {
            current,
            reading,
            ready,
            tolerance: 0.1,
            per_ready_replica: per_ready,
        }
    }

    fn spec(value: Value) -> StrategySpec {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn target_value_hpa_formula_and_tolerance_table_test() {
        let tv = TargetValue { target: 0.7 };
        // (current, reading, ready, per_ready, desired, within_tolerance)
        let cases = [
            // ceil(4 × 0.91 / 0.70) = ceil(5.2) = 6
            (4, 0.91, 4, true, 6, false),
            // exactly at target
            (4, 0.70, 4, true, 4, true),
            // inside ±10%: 0.66 / 0.70 = 0.943
            (4, 0.66, 4, true, 4, true),
            // near the edge of the band: 0.765 / 0.70 = 1.093 holds
            (4, 0.765, 4, true, 4, true),
            // just outside: 0.78 / 0.70 = 1.114 → ceil(4.457) = 5
            (4, 0.78, 4, true, 5, false),
            // scale in: ceil(8 × 0.2 / 0.7) = ceil(2.29) = 3
            (8, 0.20, 8, true, 3, false),
            // idle: 0
            (3, 0.0, 3, true, 0, false),
            // above the reservation is valid and scales out
            (2, 2.25, 2, true, 7, false),
            // not a utilization: ceil(current × reading / target)
            (5, 1.4, 0, false, 10, false),
            (5, 0.72, 0, false, 5, true),
        ];
        for (current, reading, ready, per_ready, desired, within) in cases {
            let rec = tv.recommend(&input(current, reading, ready, per_ready));
            assert_eq!(rec.desired, desired, "{current} @ {reading}: {rec:?}");
            assert_eq!(
                rec.within_tolerance, within,
                "{current} @ {reading}: {rec:?}"
            );
        }
        let rec = tv.recommend(&input(4, 0.91, 4, true));
        assert_eq!(rec.detail, "0.91 vs 0.70 (4 ready)");
    }

    #[test]
    fn target_value_warmup_rule_test() {
        let tv = TargetValue { target: 0.7 };
        // 6 desired, 4 ready at 0.91, 2 warming. Scale-out: warming count as
        // 0% busy: (0.91 × 4) / 6 = 0.607 → reversed → hold.
        let rec = tv.recommend(&input(6, 0.91, 4, true));
        assert_eq!(rec.desired, 6, "{rec:?}");
        assert!(rec.within_tolerance);
        assert!(
            rec.detail.contains("4 ready, 2 not ready"),
            "{}",
            rec.detail
        );

        // A real spike survives the rule: 4 ready at 1.8, 2 warming:
        // (1.8 × 4) / 6 = 1.2 → 1.2/0.7 × 6 = 10.29 → 11. Without the rule
        // (warming at the ready average) it would be ceil(6 × 1.8/0.7) = 16.
        let rec = tv.recommend(&input(6, 1.8, 4, true));
        assert_eq!(rec.desired, 11, "{rec:?}");

        // Scale-in: warming count as exactly at target.
        // 4 ready at 0.1, 2 warming: (0.4 + 1.4) / 6 = 0.3 → 0.3/0.7 × 6 = 2.57 → 3.
        // Without the rule (ignoring warming): ceil(4 × 0.1/0.7) = 1.
        let rec = tv.recommend(&input(6, 0.1, 4, true));
        assert_eq!(rec.desired, 3, "{rec:?}");

        // Scale-in that the warming replicas pull into the band holds.
        // 2 ready at 0.5, 4 warming: (1.0 + 2.8)/6 = 0.633 → 0.905 → within.
        let rec = tv.recommend(&input(6, 0.5, 2, true));
        assert_eq!(rec.desired, 6, "{rec:?}");
        assert!(rec.within_tolerance);

        // Nothing ready: nothing to measure, hold.
        let rec = tv.recommend(&input(3, 0.0, 0, true));
        assert_eq!(rec.desired, 3);
    }

    #[test]
    fn per_replica_threshold_pass_through_fixed_table_test() {
        let per = PerReplica { value: 500.0 };
        let threshold = Threshold {
            steps: vec![
                ThresholdStep {
                    above: 500.0,
                    add: 4,
                },
                ThresholdStep {
                    above: 250.0,
                    add: 2,
                },
                ThresholdStep {
                    above: -1.0,
                    add: -1,
                },
            ],
        };
        let fixed = Fixed { value: 3 };
        let cases: Vec<(&dyn Strategy, u32, f64, u32)> = vec![
            (&per, 1, 2300.0, 5),
            (&per, 1, 2500.0, 5),
            (&per, 9, 0.0, 0),
            (&per, 1, 1.0, 1),
            (&threshold, 4, 640.0, 8),
            (&threshold, 4, 300.0, 6),
            (&threshold, 4, 250.0, 3),
            (&threshold, 0, 10.0, 0),
            (&PassThrough {}, 2, 7.0, 7),
            (&PassThrough {}, 2, 6.2, 7),
            (&PassThrough {}, 2, -3.0, 0),
            (&fixed, 9, 123.0, 3),
        ];
        for (strategy, current, reading, desired) in cases {
            let rec = strategy.recommend(&input(current, reading, current, false));
            assert_eq!(
                rec.desired,
                desired,
                "{} {current} @ {reading}: {rec:?}",
                strategy.name()
            );
        }
        let below = Threshold {
            steps: vec![ThresholdStep {
                above: 250.0,
                add: 2,
            }],
        };
        let rec = below.recommend(&input(4, 100.0, 4, false));
        assert_eq!(rec.desired, 4);
        assert_eq!(rec.detail, "100 is below every threshold");
        assert!(!fixed.needs_reading());
        assert!(per.needs_reading());
    }

    #[test]
    fn strategy_config_parses_and_round_trips_test() {
        let cases = [
            json!({ "name": "target-value", "target": 0.7 }),
            json!({ "name": "per-replica", "value": 500.0 }),
            json!({ "name": "threshold", "steps": [{ "above": 250.0, "add": 2 }] }),
            json!({ "name": "pass-through" }),
            json!({ "name": "fixed", "value": 3 }),
            json!({ "name": "calendar", "lead": "15m", "replicas": 6 }),
        ];
        for value in cases {
            let config = StrategyConfig::from_spec(&spec(value.clone())).unwrap();
            assert_eq!(serde_json::to_value(&config).unwrap(), value);
            let back: StrategyConfig = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(back, config, "{value}");
        }
        // Any valid plugin name is a strategy plugin; whether it is
        // installed is the manager's question.
        let plugin = StrategyConfig::from_spec(&spec(json!({ "name": "calendar", "replicas": 6 })))
            .unwrap();
        assert_eq!(plugin.to_string(), "plugin calendar");
        assert!(!plugin.needs_reading());
        assert_eq!(
            plugin.plugin().map(PluginStrategy::config_value),
            Some(json!({ "replicas": 6 }))
        );
        // A built-in's typo is not a plugin: its settings are checked.
        let err: Result<StrategyConfig, _> =
            serde_json::from_value(json!({ "name": "per-replica", "valu": 5 }));
        assert!(err.is_err());
    }

    #[test]
    fn strategy_config_errors_table_test() {
        let cases = [
            (json!({ "name": "Magic" }), "unknown strategy 'Magic'"),
            (json!({ "name": "per_replica" }), "any other name is a strategy plugin"),
            (json!({ "name": "per-replica" }), "missing field `value`"),
            (
                json!({ "name": "per-replica", "value": 0 }),
                "must be greater than 0",
            ),
            (
                json!({ "name": "target-value", "target": -1 }),
                "must be greater than 0",
            ),
            (json!({ "name": "threshold", "steps": [] }), "at least one"),
            (
                json!({ "name": "fixed", "value": 3, "extra": 1 }),
                "unknown field `extra`",
            ),
        ];
        for (value, expected) in cases {
            let err = StrategyConfig::from_spec(&spec(value.clone())).unwrap_err();
            assert!(err.contains(expected), "{value}: {err}");
        }
        let empty = StrategySpec {
            name: "pass-through".to_string(),
            config: BTreeMap::new(),
        };
        assert!(StrategyConfig::from_spec(&empty).is_ok());
    }

    #[test]
    fn to_count_and_fmt_num_test() {
        assert_eq!(to_count(4.000_000_000_1), 4);
        assert_eq!(to_count(4.01), 5);
        assert_eq!(to_count(-1.0), 0);
        assert_eq!(to_count(f64::NAN), 0);
        assert_eq!(fmt_num(500.0), "500");
        assert_eq!(fmt_num(0.5), "0.5");
        assert_eq!(fmt_num(0.916), "0.92");
    }
}
