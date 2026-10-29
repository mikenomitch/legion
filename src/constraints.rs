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

    /// The first rule that fails on `node`, in words (`region = "eu"`), for
    /// diagnostics such as `GetCapacityDemand`'s refusals. `distinct` is
    /// judged as [`Self::is_satisfied`] judges it without a tracker (the
    /// key must be present). `None` when every rule holds.
    pub fn unsatisfied_rule(&self, node: &Node) -> Option<String> {
        self.rules
            .iter()
            .find(|rule| !rule.matches(node, None))
            .map(Rule::describe)
    }
}

/// The node config attribute holding the namespaces a node accepts (its
/// worker type's `namespaces`): a JSON array of names and `*`-globs. Absent
/// means every namespace. See [`accepts_namespace`].
pub const NODE_NAMESPACES_ATTR: &str = "namespaces";

/// What a task brings to a node's restrictions: its permit, and the
/// namespace of its job.
///
/// A node restricts who may land on it two ways. Its pool (plus any
/// `restriction` block) is a taint the task's permit must tolerate. Its
/// `namespaces` list is a restriction no permit can speak to: only a task
/// whose job lives in an accepted namespace passes, so a job cannot opt
/// itself onto a node reserved for another namespace (docs/namespaces.md,
/// "Nodes that accept only some namespaces").
#[derive(Debug, Clone, Default)]
pub struct PermitSet {
    rules: Vec<Rule>,
    namespace: String,
}

impl PermitSet {
    /// The permit of a task whose job lives in `namespace`.
    pub fn new(value: &Value, namespace: &str) -> R<Self> {
        let mut rules = parse_rules(value, false)?;
        if !rules.iter().any(|rule| rule.key == "pool") {
            rules.push(simple_rule("pool", Value::String("default".into())));
        }
        Ok(Self {
            rules,
            namespace: namespace.to_string(),
        })
    }

    /// The namespace this permit's task belongs to.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Whether the task may land on `node`: its permit tolerates the node's
    /// pool and restrictions, and the node accepts its job's namespace.
    pub fn is_permitted(&self, node: &Node) -> bool {
        self.permits_restrictions(node) && accepts_namespace(node, &self.namespace)
    }

    /// [`Self::is_permitted`] without the namespace check: what the permit
    /// alone says. For the diagnostic that names nodes only the namespace
    /// restriction turned away.
    pub fn permits_restrictions(&self, node: &Node) -> bool {
        // A node's restrictions are its pool plus whatever its `restriction`
        // config block declares. Almost no node declares one, and for those
        // the restriction map would hold exactly one entry — so build no map
        // at all: answer both checks (every rule holds against the
        // restrictions; every restriction is matched by some rule) straight
        // from the pool string. This runs once per node per task on the
        // candidate scan, so a map allocation here was a measurable share of
        // candidate construction at fleet scale. The general path below is
        // unchanged and gives the same answer whenever it is taken.
        let declared = node
            .config_attrs
            .as_object()
            .and_then(|attrs| attrs.get("restriction"))
            .and_then(Value::as_object)
            .filter(|block| !block.is_empty());
        if declared.is_none() {
            let holds = self.rules.iter().all(|rule| {
                if rule.key == "pool" {
                    match rule.operator {
                        Operator::IsSet => true,
                        Operator::IsNotSet => false,
                        _ => rule.matches_pool(&node.pool),
                    }
                } else {
                    // The only attribute is `pool`, so every other key is
                    // unset: `is_not_set` holds, nothing else can.
                    rule.operator == Operator::IsNotSet
                }
            });
            return holds
                && self
                    .rules
                    .iter()
                    .any(|rule| rule.key == "pool" && rule.matches_pool(&node.pool));
        }

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

/// [`PermitSet::pool_binding`] straight from a permit, for callers that name
/// a task's pool but never test a node (blocked-eval keys).
pub fn permit_pool_binding(permit: &Value) -> Option<Value> {
    PermitSet::new(permit, "")
        .ok()
        .and_then(|permit| permit.pool_binding().cloned())
}

/// The namespaces `node` accepts, when its worker type restricts them: the
/// patterns of its [`NODE_NAMESPACES_ATTR`] attribute. `None` for a node
/// that accepts every namespace (the attribute absent or empty).
pub fn node_namespaces(node: &Node) -> Option<Vec<&str>> {
    let patterns: Vec<&str> = match node.config_attrs.as_object()?.get(NODE_NAMESPACES_ATTR)? {
        Value::Array(items) => items.iter().filter_map(Value::as_str).collect(),
        Value::String(one) => vec![one.as_str()],
        _ => return None,
    };
    (!patterns.is_empty()).then_some(patterns)
}

/// Whether `node` accepts jobs from `namespace`. A node without a
/// `namespaces` list accepts every namespace. A node with one accepts the
/// namespaces its patterns match under the reserved-prefix rule
/// ([`crate::structs::acl::namespace_glob_match`]): `["*"]` accepts every
/// tenant namespace but not `legion-system`, which takes a pattern starting
/// `legion-`.
pub fn accepts_namespace(node: &Node, namespace: &str) -> bool {
    // The common node declares no list: one map lookup, no allocation.
    let Some(declared) = node
        .config_attrs
        .as_object()
        .and_then(|attrs| attrs.get(NODE_NAMESPACES_ATTR))
    else {
        return true;
    };
    let matches = |pattern: &Value| {
        pattern
            .as_str()
            .is_some_and(|pattern| crate::structs::acl::namespace_glob_match(pattern, namespace))
    };
    match declared {
        Value::Array(items) if !items.is_empty() => items.iter().any(matches),
        Value::String(_) => matches(declared),
        _ => true,
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

/// What a task's permit says about its node pool, for admission checks
/// that must know where a task may land (namespace policies).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermitPool {
    /// No `pool` rule: the implicit `pool = "default"`.
    Implicit,
    /// Exactly one pool, by plain `=` rules.
    Exact(String),
    /// A pool rule with another operator (`!=`, `matches`, ...), or rules
    /// naming different pools: not provable from the spec alone.
    Other,
}

/// Read a permit's pool requirement (see [`PermitPool`]).
pub fn permit_pool(permit: &Value) -> R<PermitPool> {
    let rules = parse_rules(permit, false)?;
    let mut exact: Option<String> = None;
    for rule in rules.iter().filter(|rule| rule.key == "pool") {
        let value = match (&rule.operator, rule.value.as_ref().and_then(Value::as_str)) {
            (Operator::Eq, Some(value)) => value,
            _ => return Ok(PermitPool::Other),
        };
        match &exact {
            Some(seen) if seen != value => return Ok(PermitPool::Other),
            _ => exact = Some(value.to_string()),
        }
    }
    Ok(exact.map_or(PermitPool::Implicit, PermitPool::Exact))
}

/// `permit` with a `pool = <pool>` rule added, in the permit's own form.
pub fn with_permit_pool(permit: &Value, pool: &str) -> Value {
    match permit {
        Value::Array(items) => {
            let mut items = items.clone();
            items.push(serde_json::json!({ "key": "pool", "value": pool }));
            Value::Array(items)
        }
        Value::Object(map) => {
            let mut map = map.clone();
            map.insert("pool".into(), Value::String(pool.to_string()));
            Value::Object(map)
        }
        _ => serde_json::json!({ "pool": pool }),
    }
}

/// `permit` with a rule admitting any of `pools` added (`pool matches
/// ^(a|b)$`). An object-shaped permit becomes the equivalent list, since the
/// object shorthand only states equality.
pub fn with_permit_pools(permit: &Value, pools: &[&str]) -> Value {
    let alternatives: Vec<String> = pools.iter().map(|pool| regex::escape(pool)).collect();
    let rule = serde_json::json!({
        "key": "pool",
        "operator": "matches",
        "value": format!("^({})$", alternatives.join("|")),
    });
    let mut items = match permit {
        Value::Array(items) => items.clone(),
        Value::Object(map) => map
            .iter()
            .map(|(key, value)| serde_json::json!({ "key": key, "value": value }))
            .collect(),
        _ => Vec::new(),
    };
    items.push(rule);
    Value::Array(items)
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
    /// The rule as written: `key operator value`.
    fn describe(&self) -> String {
        let operator = match self.operator {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Gt => ">",
            Operator::Gte => ">=",
            Operator::Lt => "<",
            Operator::Lte => "<=",
            Operator::Distinct => "distinct",
            Operator::Matches => "matches",
            Operator::VersionMatches => "version_matches",
            Operator::Contains => "contains",
            Operator::ContainsAny => "contains_any",
            Operator::IsSet => "is_set",
            Operator::IsNotSet => "is_not_set",
        };
        match &self.value {
            Some(value) => format!("{} {operator} {value}", self.key),
            None => format!("{} {operator}", self.key),
        }
    }

    fn matches(&self, node: &Node, distinct: Option<&DistinctTracker>) -> bool {
        self.matches_attr(node_attr(node, &self.key), distinct)
    }

    fn matches_attrs(
        &self,
        attrs: &serde_json::Map<String, Value>,
        distinct: Option<&DistinctTracker>,
    ) -> bool {
        self.matches_attr(attrs.get(&self.key), distinct)
    }

    /// The rule against an already-resolved attribute (`None` when the
    /// key is absent): `is_set`/`is_not_set` answer on presence, everything
    /// else needs a value. The one body behind both lookups above.
    fn matches_attr(&self, attr: Option<&Value>, distinct: Option<&DistinctTracker>) -> bool {
        match self.operator {
            Operator::IsSet => return attr.is_some_and(|value| !value.is_null()),
            Operator::IsNotSet => return attr.is_none_or(|value| value.is_null()),
            _ => {}
        }

        let Some(attr) = attr else {
            return false;
        };

        self.matches_value(attr, distinct)
    }

    /// [`Self::matches_value`] against a node's pool without materializing
    /// the pool as a JSON value: the `=`/`!=` every permit carries compare
    /// strings directly (a string value equals `Value::String` exactly when
    /// the strings are equal), and the remaining operators — rare in a
    /// permit — take the general matcher on a temporary value.
    fn matches_pool(&self, pool: &str) -> bool {
        match self.operator {
            Operator::Eq => self.value.as_ref().and_then(Value::as_str) == Some(pool),
            Operator::NotEq => self.value.as_ref().and_then(Value::as_str) != Some(pool),
            _ => self.matches_value(&Value::String(pool.to_string()), None),
        }
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

    /// The allocation-free permit path (a node with no `restriction`
    /// block) must answer exactly as the general path does. The oracle is
    /// the general path itself: a node whose restriction block re-declares
    /// its own pool builds the same one-entry restriction map the
    /// block-less node stands for, so the two must always agree.
    #[test]
    fn permit_fast_path_agrees_with_the_restriction_map_path() -> R<()> {
        let permits = [
            json!({}),
            json!({ "pool": "default" }),
            json!({ "pool": "gpu" }),
            json!([{ "key": "pool", "operator": "!=", "value": "default" }]),
            json!([{ "key": "pool", "operator": "matches", "value": "^gp" }]),
            json!([{ "key": "pool", "operator": "is_set" }]),
            json!([{ "key": "pool", "operator": "is_not_set" }]),
            json!([{ "key": "team", "operator": "is_not_set" }]),
            json!([{ "key": "team", "operator": "is_set" }]),
            json!({ "team": "a" }),
            json!([{ "key": "pool", "operator": ">", "value": 1 }]),
            json!({ "pool": 7 }),
        ];
        let pools = ["default", "gpu", "batch"];
        for permit in &permits {
            let permit_set = PermitSet::new(permit, "default")?;
            for pool in pools {
                let mut bare = make_node(json!({ "tier": "web" }));
                bare.pool = pool.into();
                let mut oracle =
                    make_node(json!({ "tier": "web", "restriction": { "pool": pool } }));
                oracle.pool = pool.into();
                let mut empty_block = make_node(json!({ "restriction": {} }));
                empty_block.pool = pool.into();
                let expected = permit_set.is_permitted(&oracle);
                assert_eq!(
                    permit_set.is_permitted(&bare),
                    expected,
                    "permit {permit} on pool {pool}"
                );
                assert_eq!(
                    permit_set.is_permitted(&empty_block),
                    expected,
                    "permit {permit} on pool {pool} with an empty block"
                );
            }
        }
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

    /// A namespace-restricted node turns away other namespaces whatever the
    /// permit says: the restriction is the node's, and a job cannot opt in.
    #[test]
    fn namespace_restricted_nodes_refuse_other_namespaces_regardless_of_permit() -> R<()> {
        let mut system = make_node(json!({ "namespaces": ["legion-system"] }));
        system.pool = "legion-system".into();
        let permits_pool = json!({ "pool": "legion-system" });
        assert!(PermitSet::new(&permits_pool, "legion-system")?.is_permitted(&system));
        let tenant = PermitSet::new(&permits_pool, "default")?;
        assert!(
            tenant.permits_restrictions(&system),
            "the pool alone admits it"
        );
        assert!(!tenant.is_permitted(&system), "the namespace does not");
        // A job in legion-system still needs a permit for the node's pool.
        assert!(!PermitSet::new(&json!({}), "legion-system")?.is_permitted(&system));

        // An unrestricted node accepts every namespace, legion-system too.
        let open = make_node(json!({}));
        assert!(PermitSet::new(&json!({}), "legion-system")?.is_permitted(&open));
        assert!(accepts_namespace(&open, "payments"));
        assert_eq!(node_namespaces(&open), None);
        Ok(())
    }

    /// Node-side lists follow the reserved rule: `["*"]` accepts every
    /// tenant namespace but not `legion-system`.
    #[test]
    fn star_node_lists_do_not_accept_reserved_namespaces() {
        let star = make_node(json!({ "namespaces": ["*"] }));
        assert!(accepts_namespace(&star, "default"));
        assert!(accepts_namespace(&star, "payments-prod"));
        assert!(!accepts_namespace(&star, "legion-system"));
        let both = make_node(json!({ "namespaces": ["*", "legion-*"] }));
        assert!(accepts_namespace(&both, "legion-system"));
        assert!(accepts_namespace(&both, "default"));
        let ci = make_node(json!({ "namespaces": ["ci-*"] }));
        assert!(accepts_namespace(&ci, "ci-main"));
        assert!(!accepts_namespace(&ci, "default"));
        assert_eq!(node_namespaces(&ci), Some(vec!["ci-*"]));
    }
}
