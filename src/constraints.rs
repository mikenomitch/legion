use std::collections::{HashMap, HashSet};

use regex::Regex;
use semver::{Version, VersionReq};
use serde_json::Value;

use crate::prelude::*;
use crate::structs::Node;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
    Distinct,
    Matches,
    VersionMatches,
    Contains,
    ContainsAny,
    IsSet,
    IsNotSet,
}

#[derive(Debug, Clone)]
enum Matcher {
    Regex(Regex),
    VersionReq(VersionReq),
}

#[derive(Debug, Clone)]
struct Rule {
    key: String,
    operator: Operator,
    value: Option<Value>,
    matcher: Option<Matcher>,
    multiplier: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ConstraintSet {
    rules: Vec<Rule>,
}

#[derive(Debug, Clone, Default)]
pub struct AffinitySet {
    rules: Vec<Rule>,
}

#[derive(Debug, Clone, Default)]
pub struct DistinctTracker {
    seen: HashMap<String, HashSet<String>>,
}

impl ConstraintSet {
    pub fn new(value: &Value) -> R<Self> {
        let rules = parse_rules(value, false)?;
        Ok(Self { rules })
    }

    pub fn is_satisfied(&self, node: &Node, distinct: Option<&DistinctTracker>) -> bool {
        self.rules.iter().all(|rule| rule.matches(node, distinct))
    }

    pub fn distinct_satisfied(&self, node: &Node, distinct: &DistinctTracker) -> bool {
        self.rules
            .iter()
            .filter(|rule| rule.operator == Operator::Distinct)
            .all(|rule| rule.matches(node, Some(distinct)))
    }

    pub fn distinct_keys(&self) -> Vec<String> {
        self.rules
            .iter()
            .filter(|rule| rule.operator == Operator::Distinct)
            .map(|rule| rule.key.clone())
            .collect()
    }

    /// Keys this constraint set pins to an exact value: every `=` rule with a
    /// concrete value. Used by scheduling-index selection to decide which
    /// `partition_by` keys a task binds; the full rule set is always
    /// re-verified on candidates, so this only has to be sound, not complete.
    /// When the same key carries several `=` rules the first wins here —
    /// conflicting values make every candidate fail re-verification anyway.
    pub fn equality_bindings(&self) -> HashMap<String, Value> {
        let mut bindings = HashMap::new();
        for rule in &self.rules {
            if rule.operator != Operator::Eq {
                continue;
            }
            let Some(value) = rule.value.as_ref() else {
                continue;
            };
            bindings
                .entry(rule.key.clone())
                .or_insert_with(|| value.clone());
        }
        bindings
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

#[derive(Debug, Clone, Default)]
pub struct PermitSet {
    rules: Vec<Rule>,
}

impl PermitSet {
    pub fn new(value: &Value) -> R<Self> {
        let mut rules = parse_rules(value, false)?;
        if !rules.iter().any(|rule| rule.key == "pool") {
            rules.push(simple_rule("pool", Value::String("default".into())));
        }
        Ok(Self { rules })
    }

    pub fn is_permitted(&self, node: &Node) -> bool {
        let restrictions = node_restrictions(node);

        if !self
            .rules
            .iter()
            .all(|rule| rule.matches_attrs(&restrictions, None))
        {
            return false;
        }

        restrictions.iter().all(|(key, value)| {
            self.rules
                .iter()
                .any(|rule| rule.key == *key && rule.matches_value(value, None))
        })
    }

    /// The exact pool this permit requires, when it requires one with a plain
    /// `=` rule (which includes the implicit `pool=default`). `None` when the
    /// pool rule uses a non-equality operator — scheduling-index selection
    /// then treats `pool` as unbound and falls back to the scan.
    pub fn pool_binding(&self) -> Option<&Value> {
        self.rules
            .iter()
            .find(|rule| rule.key == "pool" && rule.operator == Operator::Eq)
            .and_then(|rule| rule.value.as_ref())
    }
}

impl AffinitySet {
    pub fn new(value: &Value) -> R<Self> {
        let rules = parse_rules(value, true)?;
        Ok(Self { rules })
    }

    pub fn score(&self, node: &Node) -> f64 {
        if self.rules.is_empty() {
            return 0.0;
        }

        let mut matched = 0.0;
        let mut total = 0.0;

        for rule in &self.rules {
            let weight = rule.multiplier.abs();
            total += weight;
            if rule.matches(node, None) {
                matched += rule.multiplier;
            }
        }

        if total == 0.0 {
            0.0
        } else {
            matched / total
        }
    }
}

impl DistinctTracker {
    pub fn new(keys: &[String]) -> Self {
        let mut tracker = DistinctTracker::default();
        for key in keys {
            tracker.seen.insert(key.clone(), HashSet::new());
        }
        tracker
    }

    pub fn insert_from_node(&mut self, node: &Node) {
        for (key, seen) in &mut self.seen {
            if let Some(value) = node_attr(node, key) {
                seen.insert(value_token(value));
            }
        }
    }

    pub fn contains(&self, key: &str, value: &Value) -> bool {
        self.seen
            .get(key)
            .map(|seen| seen.contains(&value_token(value)))
            .unwrap_or(false)
    }
}

fn parse_rules(value: &Value, allow_multiplier: bool) -> R<Vec<Rule>> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Object(map) if map.is_empty() => Ok(Vec::new()),
        Value::Object(map) => map
            .iter()
            .map(|(key, value)| Ok(simple_rule(key, value.clone())))
            .collect(),
        Value::Array(items) => items
            .iter()
            .map(|item| parse_rule_object(item, allow_multiplier))
            .collect(),
        _ => bail!("constraints/affinities must be an object or array"),
    }
}

fn simple_rule(key: &str, value: Value) -> Rule {
    Rule {
        key: key.to_string(),
        operator: Operator::Eq,
        value: Some(value),
        matcher: None,
        multiplier: 1.0,
    }
}

fn parse_rule_object(value: &Value, allow_multiplier: bool) -> R<Rule> {
    let obj = value
        .as_object()
        .context("constraint/affinity entries must be objects")?;

    let key = obj
        .get("key")
        .and_then(|value| value.as_str())
        .context("constraint/affinity entries must include key")?;

    let operator = match obj.get("operator") {
        Some(raw) => parse_operator(raw)?,
        None => Operator::Eq,
    };

    let multiplier = match (allow_multiplier, obj.get("multiplier")) {
        (true, Some(value)) => value
            .as_f64()
            .context("affinity multiplier must be a number")?,
        _ => 1.0,
    };

    let value = obj.get("value").cloned();
    if value.is_none()
        && !matches!(
            operator,
            Operator::Distinct | Operator::IsSet | Operator::IsNotSet
        )
    {
        bail!("constraint/affinity entry {key} requires a value");
    }

    let matcher = match operator {
        Operator::Matches => {
            let pattern = value
                .as_ref()
                .and_then(|value| value.as_str())
                .context("matches operator expects string value")?;
            Some(Matcher::Regex(Regex::new(pattern)?))
        }
        Operator::VersionMatches => {
            let requirement = value
                .as_ref()
                .and_then(|value| value.as_str())
                .context("version_matches operator expects string value")?;
            Some(Matcher::VersionReq(VersionReq::parse(requirement)?))
        }
        _ => None,
    };

    Ok(Rule {
        key: key.to_string(),
        operator,
        value,
        matcher,
        multiplier,
    })
}

fn parse_operator(value: &Value) -> R<Operator> {
    let raw = value
        .as_str()
        .context("constraint/affinity operator must be a string")?;

    match raw {
        "equal" => Ok(Operator::Eq),
        "=" => Ok(Operator::Eq),
        "!=" => Ok(Operator::NotEq),
        ">" => Ok(Operator::Gt),
        ">=" => Ok(Operator::Gte),
        "<" => Ok(Operator::Lt),
        "<=" => Ok(Operator::Lte),
        "distinct" => Ok(Operator::Distinct),
        "matches" => Ok(Operator::Matches),
        "version_matches" => Ok(Operator::VersionMatches),
        "contains" => Ok(Operator::Contains),
        "contains_any" => Ok(Operator::ContainsAny),
        "is_set" => Ok(Operator::IsSet),
        "is_not_set" => Ok(Operator::IsNotSet),
        _ => bail!("unknown constraint/affinity operator: {raw}"),
    }
}

impl Rule {
    fn matches(&self, node: &Node, distinct: Option<&DistinctTracker>) -> bool {
        match self.operator {
            Operator::IsSet => {
                return node_attr(node, &self.key).is_some_and(|value| !value.is_null())
            }
            Operator::IsNotSet => {
                return node_attr(node, &self.key).is_none_or(|value| value.is_null())
            }
            _ => {}
        }

        let Some(attr) = node_attr(node, &self.key) else {
            return false;
        };

        self.matches_value(attr, distinct)
    }

    fn matches_attrs(
        &self,
        attrs: &serde_json::Map<String, Value>,
        distinct: Option<&DistinctTracker>,
    ) -> bool {
        match self.operator {
            Operator::IsSet => return attrs.get(&self.key).is_some_and(|value| !value.is_null()),
            Operator::IsNotSet => return attrs.get(&self.key).is_none_or(|value| value.is_null()),
            _ => {}
        }

        let Some(attr) = attrs.get(&self.key) else {
            return false;
        };

        self.matches_value(attr, distinct)
    }

    fn matches_value(&self, attr: &Value, distinct: Option<&DistinctTracker>) -> bool {
        match self.operator {
            Operator::Eq => attr == self.value.as_ref().unwrap(),
            Operator::NotEq => attr != self.value.as_ref().unwrap(),
            Operator::Gt => compare_numbers(attr, self.value.as_ref().unwrap(), |a, b| a > b),
            Operator::Gte => compare_numbers(attr, self.value.as_ref().unwrap(), |a, b| a >= b),
            Operator::Lt => compare_numbers(attr, self.value.as_ref().unwrap(), |a, b| a < b),
            Operator::Lte => compare_numbers(attr, self.value.as_ref().unwrap(), |a, b| a <= b),
            Operator::Distinct => {
                if let Some(tracker) = distinct {
                    !tracker.contains(&self.key, attr)
                } else {
                    true
                }
            }
            Operator::Matches => match &self.matcher {
                Some(Matcher::Regex(regex)) => attr
                    .as_str()
                    .map(|value| regex.is_match(value))
                    .unwrap_or(false),
                _ => false,
            },
            Operator::VersionMatches => match &self.matcher {
                Some(Matcher::VersionReq(req)) => attr
                    .as_str()
                    .and_then(|value| Version::parse(value).ok())
                    .map(|version| req.matches(&version))
                    .unwrap_or(false),
                _ => false,
            },
            Operator::Contains => value_contains(attr, self.value.as_ref().unwrap()),
            Operator::ContainsAny => value_contains_any(attr, self.value.as_ref().unwrap()),
            Operator::IsSet | Operator::IsNotSet => false,
        }
    }
}

fn compare_numbers<F>(left: &Value, right: &Value, comparator: F) -> bool
where
    F: Fn(f64, f64) -> bool,
{
    match (left.as_f64(), right.as_f64()) {
        (Some(left), Some(right)) => comparator(left, right),
        _ => false,
    }
}

fn value_contains(haystack: &Value, needle: &Value) -> bool {
    match (haystack, needle) {
        (Value::Array(values), Value::Array(targets)) => targets
            .iter()
            .all(|target| values.iter().any(|value| value == target)),
        (Value::Array(values), _) => values.iter().any(|value| value == needle),
        (Value::String(haystack), Value::String(needle)) => haystack.contains(needle),
        _ => false,
    }
}

fn value_contains_any(haystack: &Value, needle: &Value) -> bool {
    match (haystack, needle) {
        (Value::Array(values), Value::Array(targets)) => targets
            .iter()
            .any(|target| values.iter().any(|value| value == target)),
        (Value::Array(values), _) => values.iter().any(|value| value == needle),
        (Value::String(haystack), Value::Array(targets)) => targets.iter().any(|target| {
            target
                .as_str()
                .map(|target| haystack.contains(target))
                .unwrap_or(false)
        }),
        (Value::String(haystack), Value::String(needle)) => haystack.contains(needle),
        _ => false,
    }
}

/// Node attribute lookup in the documented precedence order:
/// `config_attrs`, then `fingerprint_attrs`, then `dynamic_attrs`. Shared
/// with the scheduling index so both agree on what a key resolves to.
pub(crate) fn node_attr<'a>(node: &'a Node, key: &str) -> Option<&'a Value> {
    node.config_attrs
        .as_object()
        .and_then(|attrs| attrs.get(key))
        .or_else(|| {
            node.fingerprint_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
        .or_else(|| {
            node.dynamic_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
}

/// Canonical string token for a JSON value, shared with the scheduling
/// index's partition keys so token equality tracks constraint `=` equality.
pub(crate) fn value_token(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| String::new())
}

fn node_restrictions(node: &Node) -> serde_json::Map<String, Value> {
    let mut restrictions = serde_json::Map::new();

    restrictions.insert("pool".into(), Value::String(node.pool.clone()));

    if let Some(Value::Object(map)) = node
        .config_attrs
        .as_object()
        .and_then(|attrs| attrs.get("restriction"))
    {
        for (key, value) in map {
            restrictions.insert(key.clone(), value.clone());
        }
    }

    restrictions
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn make_node(attrs: Value) -> Node {
        Node {
            id: "node-a".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
            heartbeat_source: String::new(),
            config_attrs: attrs,
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    #[test]
    fn constraint_map_defaults_to_equal() -> R<()> {
        let constraints = ConstraintSet::new(&json!({ "role": "db" }))?;
        let node = make_node(json!({ "role": "db" }));

        assert!(constraints.is_satisfied(&node, None));

        Ok(())
    }

    #[test]
    fn supports_comparison_operators() -> R<()> {
        let constraints = ConstraintSet::new(&json!([
            { "key": "count", "value": 5, "operator": ">" },
            { "key": "count", "value": 10, "operator": "<=" }
        ]))?;
        let node = make_node(json!({ "count": 7 }));

        assert!(constraints.is_satisfied(&node, None));

        Ok(())
    }

    #[test]
    fn supports_contains_variants() -> R<()> {
        let constraints = ConstraintSet::new(&json!([
            { "key": "tags", "value": ["a", "b"], "operator": "contains" },
            { "key": "tags", "value": ["c", "d"], "operator": "contains_any" }
        ]))?;
        let node = make_node(json!({ "tags": ["a", "b", "c"] }));

        assert!(constraints.is_satisfied(&node, None));

        Ok(())
    }

    #[test]
    fn supports_matches_and_version_matches() -> R<()> {
        let constraints = ConstraintSet::new(&json!([
            { "key": "arch", "value": "^arm", "operator": "matches" },
            { "key": "version", "value": ">=1.2", "operator": "version_matches" }
        ]))?;
        let node = make_node(json!({ "arch": "arm64", "version": "1.2.3" }));

        assert!(constraints.is_satisfied(&node, None));

        Ok(())
    }

    #[test]
    fn supports_is_set_operators() -> R<()> {
        let constraints = ConstraintSet::new(&json!([
            { "key": "role", "operator": "is_set" },
            { "key": "missing", "operator": "is_not_set" }
        ]))?;
        let node = make_node(json!({ "role": "db" }));

        assert!(constraints.is_satisfied(&node, None));

        Ok(())
    }

    #[test]
    fn distinct_operator_excludes_seen_values() -> R<()> {
        let constraints = ConstraintSet::new(&json!([
            { "key": "zone", "operator": "distinct" }
        ]))?;
        let mut tracker = DistinctTracker::new(&constraints.distinct_keys());
        let node = make_node(json!({ "zone": "us-east" }));
        tracker.insert_from_node(&node);

        assert!(!constraints.is_satisfied(&node, Some(&tracker)));

        Ok(())
    }

    #[test]
    fn affinity_score_respects_multipliers() -> R<()> {
        let affinities = AffinitySet::new(&json!([
            { "key": "zone", "value": "us-east", "multiplier": 2 },
            { "key": "arch", "value": "arm64", "multiplier": -1 }
        ]))?;
        let node = make_node(json!({ "zone": "us-east", "arch": "arm64" }));

        let score = affinities.score(&node);
        assert!(score < 1.0);
        assert!(score > -1.0);

        Ok(())
    }
}
