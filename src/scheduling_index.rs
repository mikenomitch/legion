//! Declarable scheduling indexes: ad-hoc, operator-defined candidate
//! generation for the scheduler (`internal_docs/plans/scheduling-index-plan.md`,
//! public doc `docs/scheduling-indexes.md`).
//!
//! An index is `partition_by` (exact-match attribute keys, each distinct
//! value-tuple is a shard) times `dims` (numeric dimensions bucketed into a
//! sparse grid — free resource capacity via `resources.*`, or any numeric
//! node attribute). A query picks the shard the task's equality constraints
//! bind, then walks the shard's occupied cells *nearest-fit first*: cells
//! whose every bucket covers the request's bucket ("the orthant"), ordered by
//! weighted bucket distance, then every remaining cell. Ordering is therefore
//! fit-informed but the untruncated candidate *set* is exactly the shard —
//! with an unbounded budget the scheduler's re-verified candidates equal
//! today's full scan, which is what the equivalence tests pin down.
//!
//! The index only ever *proposes* candidates. Every consumer re-verifies them
//! through the ordinary constraint/permit/ledger chain, so a stale or wrong
//! index costs a refill or a worse pick, never an invalid placement.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::constraints::{node_attr, value_token};
use crate::prelude::*;
use crate::resources::ResourcePolicy;
use crate::structs::{Allocation, Node};

/// Default for `schedulerSettings.index_min_nodes`: below this many candidate
/// nodes no index arms and planning uses the plain scan. `0` disables
/// indexing entirely (the documented off-ramp).
pub const DEFAULT_INDEX_MIN_NODES: usize = 128;

/// Hard cap on dims per index — cells are tiny vectors and orthant walks
/// degrade with dimensionality, so this fails fast at parse time.
pub const MAX_DIMS_PER_INDEX: usize = 6;

/// Dim attrs with this prefix read free resource capacity; anything else
/// reads the node attribute chain (config -> fingerprint -> dynamic).
const RESOURCE_DIM_PREFIX: &str = "resources.";

/// Joins per-key partition tokens into one shard key. An ASCII unit
/// separator cannot appear unescaped in `value_token` output (serde_json
/// escapes control characters), so joined tuples cannot collide.
const PARTITION_SEPARATOR: char = '\u{1f}';

/// Partition token for a node missing the partition attribute entirely.
/// Distinct from every real `value_token`, so bound queries (which always
/// carry a real value) can never land on nodes lacking the attribute.
const ABSENT_TOKEN: &str = "\u{1f}absent";

/// Log-scale bucket reserved for infinite capacity (unfingerprinted
/// resources); finite values cap one below it.
const LOG_INFINITY_BUCKET: u16 = 64;

/// How an index participates in planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMode {
    /// Candidates come from the index (scan remains the fallback).
    On,
    /// The scan stays authoritative; the index runs alongside and
    /// disagreements are surfaced via logs and metrics.
    Shadow,
    /// Declared but inert.
    Off,
}

impl IndexMode {
    pub fn as_str(self) -> &'static str {
        match self {
            IndexMode::On => "on",
            IndexMode::Shadow => "shadow",
            IndexMode::Off => "off",
        }
    }
}

/// Candidate ordering within a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOrder {
    /// Smallest sufficient fit first (bin-pack-ish).
    Tight,
    /// Most headroom first (spread-ish).
    Loose,
}

/// How a dim value maps to a bucket index.
#[derive(Debug, Clone, PartialEq)]
pub enum DimScale {
    /// `floor(log2(value + 1))`, capped at 63; infinity gets its own bucket.
    /// Unit-independent and monotone — the default, and the right choice for
    /// resource capacities spanning orders of magnitude.
    Log,
    /// `floor(value / step)` — for small discrete ranges like GPU counts.
    Linear { step: f64 },
}

/// One numeric dimension of an index.
#[derive(Debug, Clone)]
pub struct DimSpec {
    /// `resources.<name>` for free capacity, else a numeric node attribute.
    pub attr: String,
    /// Multiplies this dim's contribution to the ordering distance.
    pub weight: f64,
    pub scale: DimScale,
}

/// A parsed index declaration.
#[derive(Debug, Clone)]
pub struct IndexSpec {
    pub name: String,
    pub partition_by: Vec<String>,
    pub dims: Vec<DimSpec>,
    pub order: IndexOrder,
    pub mode: IndexMode,
}

impl IndexSpec {
    fn resource_dim_names(&self) -> impl Iterator<Item = &str> {
        self.dims
            .iter()
            .filter_map(|dim| dim.attr.strip_prefix(RESOURCE_DIM_PREFIX))
    }
}

/// Parsed `schedulerSettings.indexes` + `index_min_nodes`.
#[derive(Debug, Clone)]
pub struct IndexSettings {
    pub indexes: Vec<IndexSpec>,
    pub min_nodes: usize,
}

impl Default for IndexSettings {
    fn default() -> Self {
        IndexSettings {
            indexes: Vec::new(),
            min_nodes: DEFAULT_INDEX_MIN_NODES,
        }
    }
}

/// The built-in default index: nearest-fit over free cpu/memory/disk within
/// the task's pool. Nodes that do not declare a resource (disk, typically)
/// land in that dim's zero-free bucket — they match requests that don't ask
/// for it and sort last for requests that do, which is exactly what the
/// ledger would conclude anyway.
fn default_fit_spec(order: IndexOrder) -> IndexSpec {
    IndexSpec {
        name: "fit".to_string(),
        partition_by: vec!["pool".to_string()],
        dims: ["resources.cpu", "resources.memory", "resources.disk"]
            .iter()
            .map(|attr| DimSpec {
                attr: (*attr).to_string(),
                weight: 1.0,
                scale: DimScale::Log,
            })
            .collect(),
        order,
        mode: IndexMode::On,
    }
}

impl IndexSettings {
    /// Parses `schedulerSettings` into index settings. `scheduling_policy`
    /// supplies the default ordering: `spread` prefers headroom (`loose`),
    /// everything else packs (`tight`). Unknown fields and malformed specs
    /// fail loudly — indexes silently doing nothing is the failure mode the
    /// explain surface exists to prevent, and that starts at parse time.
    pub fn from_scheduler_settings(
        settings: Option<&Value>,
        scheduling_policy: &str,
    ) -> R<IndexSettings> {
        let default_order = if scheduling_policy == "spread" {
            IndexOrder::Loose
        } else {
            IndexOrder::Tight
        };

        let map = match settings {
            None | Some(Value::Null) => None,
            Some(Value::Object(map)) => Some(map),
            Some(_) => bail!("scheduler_settings must be an object"),
        };

        let min_nodes = match map.and_then(|map| map.get("index_min_nodes")) {
            None | Some(Value::Null) => DEFAULT_INDEX_MIN_NODES,
            Some(value) => value
                .as_u64()
                .context("index_min_nodes must be a non-negative integer")?
                as usize,
        };

        let mut indexes = Vec::new();
        let mut fit_handled = false;

        if let Some(entries) = map.and_then(|map| map.get("indexes")) {
            let entries = match entries {
                Value::Null => None,
                Value::Object(entries) => Some(entries),
                _ => bail!("scheduler_settings.indexes must be an object of name -> index"),
            };
            for (name, value) in entries.into_iter().flatten() {
                if name == "fit" {
                    fit_handled = true;
                }
                match value {
                    // `<name>: false` disables — the `fit: false` kill switch.
                    Value::Bool(false) => continue,
                    Value::Bool(true) => {
                        if name == "fit" {
                            indexes.push(default_fit_spec(default_order));
                        } else {
                            bail!("index {name}: `true` is only valid for the built-in `fit` index; declare attributes instead");
                        }
                    }
                    other => indexes.push(parse_index_spec(name, other, default_order)?),
                }
            }
        }

        if !fit_handled {
            indexes.push(default_fit_spec(default_order));
        }

        let mut seen = HashSet::new();
        for spec in &indexes {
            if !seen.insert(spec.name.as_str()) {
                bail!("duplicate index name {}", spec.name);
            }
        }

        Ok(IndexSettings { indexes, min_nodes })
    }

    /// True when any declared index can ever serve a query.
    pub fn any_active(&self) -> bool {
        self.min_nodes > 0 && self.indexes.iter().any(|spec| spec.mode != IndexMode::Off)
    }

    /// Whether indexes participate in a plan over `candidate_count` nodes:
    /// active, and at or above the `index_min_nodes` arming threshold.
    pub fn armed_for(&self, candidate_count: usize) -> bool {
        self.any_active() && candidate_count >= self.min_nodes
    }

    /// Picks the index for a task: usable means not `off` and every
    /// `partition_by` key bound by `bindings`; best coverage of the task's
    /// requested resources wins, then more partition keys, then declaration
    /// order. `Err` carries the human-readable reason for the explain surface.
    pub fn select(
        &self,
        bindings: &HashMap<String, Value>,
        request: &HashMap<String, f64>,
    ) -> Result<usize, String> {
        if self.indexes.is_empty() {
            return Err("no indexes configured".to_string());
        }
        let mut best: Option<(usize, usize, usize)> = None;
        let mut unusable = Vec::new();
        for (idx, spec) in self.indexes.iter().enumerate() {
            if spec.mode == IndexMode::Off {
                continue;
            }
            let missing: Vec<&str> = spec
                .partition_by
                .iter()
                .filter(|key| !bindings.contains_key(key.as_str()))
                .map(String::as_str)
                .collect();
            if !missing.is_empty() {
                unusable.push(format!("{} needs [{}]", spec.name, missing.join(", ")));
                continue;
            }
            let coverage = spec
                .resource_dim_names()
                .filter(|name| request.get(*name).copied().unwrap_or(0.0) > 0.0)
                .count();
            let candidate = (coverage, spec.partition_by.len(), idx);
            let better = match best {
                None => true,
                // Highest coverage, then most partition keys, then earliest
                // declaration (idx ascending — hence the inverted compare).
                Some((cov, parts, best_idx)) => {
                    (candidate.0, candidate.1, std::cmp::Reverse(candidate.2))
                        > (cov, parts, std::cmp::Reverse(best_idx))
                }
            };
            if better {
                best = Some(candidate);
            }
        }
        match best {
            Some((_, _, idx)) => Ok(idx),
            None if unusable.is_empty() => Err("every index is off".to_string()),
            None => Err(format!("no usable index: {}", unusable.join("; "))),
        }
    }
}

fn parse_index_spec(name: &str, value: &Value, default_order: IndexOrder) -> R<IndexSpec> {
    let (partition_by, dims, order, mode) = match value {
        // Shorthand: a list of attribute keys. `resources.*` entries become
        // dims, everything else partitions.
        Value::Array(items) => {
            let mut partition_by = Vec::new();
            let mut dims = Vec::new();
            for item in items {
                let key = item
                    .as_str()
                    .with_context(|| format!("index {name}: shorthand entries must be strings"))?;
                if key.starts_with(RESOURCE_DIM_PREFIX) {
                    dims.push(DimSpec {
                        attr: key.to_string(),
                        weight: 1.0,
                        scale: DimScale::Log,
                    });
                } else {
                    partition_by.push(key.to_string());
                }
            }
            (partition_by, dims, default_order, IndexMode::On)
        }
        Value::Object(map) => {
            for key in map.keys() {
                if !matches!(key.as_str(), "partition_by" | "dims" | "order" | "mode") {
                    bail!("index {name}: unknown field {key} (expected partition_by, dims, order, mode)");
                }
            }
            let partition_by = match map.get("partition_by") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(items)) => items
                    .iter()
                    .map(|item| {
                        item.as_str().map(str::to_string).with_context(|| {
                            format!("index {name}: partition_by entries must be strings")
                        })
                    })
                    .collect::<R<Vec<_>>>()?,
                Some(_) => bail!("index {name}: partition_by must be an array of strings"),
            };
            let dims = match map.get("dims") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(items)) => items
                    .iter()
                    .map(|item| parse_dim_spec(name, item))
                    .collect::<R<Vec<_>>>()?,
                Some(_) => bail!("index {name}: dims must be an array"),
            };
            let order = match map.get("order").and_then(Value::as_str) {
                None => default_order,
                Some("tight") => IndexOrder::Tight,
                Some("loose") => IndexOrder::Loose,
                Some(other) => bail!("index {name}: order must be tight or loose, got {other}"),
            };
            let mode = match map.get("mode").and_then(Value::as_str) {
                None => IndexMode::On,
                Some("on") => IndexMode::On,
                Some("shadow") => IndexMode::Shadow,
                Some("off") => IndexMode::Off,
                Some(other) => bail!("index {name}: mode must be on, shadow, or off, got {other}"),
            };
            (partition_by, dims, order, mode)
        }
        _ => bail!("index {name}: must be false, a list of attribute keys, or an object"),
    };

    if partition_by.is_empty() && dims.is_empty() {
        bail!("index {name}: needs at least one partition_by key or dim");
    }
    if dims.len() > MAX_DIMS_PER_INDEX {
        bail!(
            "index {name}: at most {MAX_DIMS_PER_INDEX} dims per index (got {})",
            dims.len()
        );
    }
    let mut seen = HashSet::new();
    for key in partition_by.iter().chain(dims.iter().map(|dim| &dim.attr)) {
        if key.is_empty() {
            bail!("index {name}: attribute keys cannot be empty");
        }
        if !seen.insert(key.as_str()) {
            bail!("index {name}: duplicate attribute {key}");
        }
    }

    Ok(IndexSpec {
        name: name.to_string(),
        partition_by,
        dims,
        order,
        mode,
    })
}

fn parse_dim_spec(index_name: &str, value: &Value) -> R<DimSpec> {
    match value {
        Value::String(attr) => Ok(DimSpec {
            attr: attr.clone(),
            weight: 1.0,
            scale: DimScale::Log,
        }),
        Value::Object(map) => {
            for key in map.keys() {
                if !matches!(key.as_str(), "attr" | "weight" | "scale" | "step") {
                    bail!("index {index_name}: unknown dim field {key} (expected attr, weight, scale, step)");
                }
            }
            let attr = map
                .get("attr")
                .and_then(Value::as_str)
                .with_context(|| format!("index {index_name}: dim entries need an attr string"))?
                .to_string();
            let weight = match map.get("weight") {
                None | Some(Value::Null) => 1.0,
                Some(value) => {
                    let weight = value.as_f64().with_context(|| {
                        format!("index {index_name}: dim weight must be a number")
                    })?;
                    if !weight.is_finite() || weight < 0.0 {
                        bail!("index {index_name}: dim weight must be finite and non-negative");
                    }
                    weight
                }
            };
            let scale = match map.get("scale").and_then(Value::as_str) {
                None | Some("log") => {
                    if map.contains_key("step") {
                        bail!("index {index_name}: step only applies to scale: linear");
                    }
                    DimScale::Log
                }
                Some("linear") => {
                    let step = match map.get("step") {
                        None | Some(Value::Null) => 1.0,
                        Some(value) => value.as_f64().with_context(|| {
                            format!("index {index_name}: dim step must be a number")
                        })?,
                    };
                    if !step.is_finite() || step <= 0.0 {
                        bail!("index {index_name}: dim step must be a positive number");
                    }
                    DimScale::Linear { step }
                }
                Some(other) => {
                    bail!("index {index_name}: dim scale must be log or linear, got {other}")
                }
            };
            Ok(DimSpec {
                attr,
                weight,
                scale,
            })
        }
        _ => bail!("index {index_name}: dims must be strings or objects"),
    }
}

// ---------------------------------------------------------------------------
// Live state
// ---------------------------------------------------------------------------

/// One node's projection into one index.
type NodeProjection = (String, Vec<u16>);

#[derive(Debug, Clone, Default)]
struct Shard {
    /// Cell -> node ids in it, ids kept sorted for deterministic output.
    cells: BTreeMap<Vec<u16>, Vec<String>>,
}

#[derive(Debug, Clone, Default)]
struct IndexState {
    shards: HashMap<String, Shard>,
    node_records: HashMap<String, NodeProjection>,
}

impl IndexState {
    fn insert(&mut self, node_id: &str, projection: NodeProjection) {
        let shard = self.shards.entry(projection.0.clone()).or_default();
        let ids = shard.cells.entry(projection.1.clone()).or_default();
        if let Err(pos) = ids.binary_search_by(|id| id.as_str().cmp(node_id)) {
            ids.insert(pos, node_id.to_string());
        }
        self.node_records.insert(node_id.to_string(), projection);
    }

    fn remove(&mut self, node_id: &str) {
        let Some((partition, cell)) = self.node_records.remove(node_id) else {
            return;
        };
        let Some(shard) = self.shards.get_mut(&partition) else {
            return;
        };
        if let Some(ids) = shard.cells.get_mut(&cell) {
            if let Ok(pos) = ids.binary_search_by(|id| id.as_str().cmp(node_id)) {
                ids.remove(pos);
            }
            if ids.is_empty() {
                shard.cells.remove(&cell);
            }
        }
        if shard.cells.is_empty() {
            self.shards.remove(&partition);
        }
    }
}

/// Result of one candidate query.
#[derive(Debug, Clone)]
pub struct IndexCandidates {
    pub index: String,
    pub mode: IndexMode,
    /// Candidate node ids, best-first per the index's order. With no budget
    /// this is the entire shard.
    pub node_ids: Vec<String>,
    /// Shard population before any budget truncation.
    pub partition_size: usize,
    pub truncated: bool,
}

/// What a query attempt produced.
#[derive(Debug, Clone)]
pub enum IndexQueryOutcome {
    Candidates(IndexCandidates),
    /// Indexes exist but none serves this task; `reason` is user-facing.
    Unusable {
        reason: String,
    },
    /// No indexes configured (or indexing globally disabled).
    Disabled,
}

/// Every declared index's live state. Lives inside the scheduling read model
/// and is maintained by the same write path that mutates nodes and
/// allocations, so it cannot drift from them. Derived state only — rebuilt
/// from the nodes table at startup and on failover, never persisted.
#[derive(Debug, Clone, Default)]
pub struct IndexSet {
    settings: Arc<IndexSettings>,
    states: Vec<IndexState>,
}

impl IndexSet {
    pub fn build(
        settings: Arc<IndexSettings>,
        nodes: &HashMap<String, Node>,
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
        policy: &ResourcePolicy,
    ) -> IndexSet {
        let mut set = IndexSet {
            states: settings
                .indexes
                .iter()
                .map(|_| IndexState::default())
                .collect(),
            settings,
        };
        for node_id in nodes.keys() {
            set.reindex_node(node_id, nodes, allocations_by_node, policy);
        }
        set
    }

    pub fn settings(&self) -> &IndexSettings {
        &self.settings
    }

    /// True when at least one index can serve queries at all (before the
    /// per-plan `min_nodes` arming check, which needs the candidate count).
    pub fn any_active(&self) -> bool {
        self.settings.any_active()
    }

    /// Re-projects one node into every index. Handles insert, update, and
    /// availability-driven removal; skips shard surgery when the projection
    /// is unchanged (the common heartbeat case).
    pub fn reindex_node(
        &mut self,
        node_id: &str,
        nodes: &HashMap<String, Node>,
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
        policy: &ResourcePolicy,
    ) {
        if self.settings.indexes.is_empty() {
            return;
        }
        let node = match nodes.get(node_id) {
            // The index holds exactly the nodes the scheduler considers:
            // available ones. Everything else is absent, same as removed.
            Some(node) if node.availability == "available" => node,
            _ => {
                self.remove_node(node_id);
                return;
            }
        };
        let needs_free = self
            .settings
            .indexes
            .iter()
            .any(|spec| spec.resource_dim_names().next().is_some());
        let free = if needs_free {
            let allocs = allocations_by_node
                .get(node_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            crate::manager::scheduler::node_free_resource_amounts(node, allocs, policy)
        } else {
            HashMap::new()
        };
        for (spec, state) in self.settings.indexes.iter().zip(self.states.iter_mut()) {
            let projection = project_node(spec, node, &free);
            if state.node_records.get(node_id) == Some(&projection) {
                continue;
            }
            state.remove(node_id);
            state.insert(node_id, projection);
        }
    }

    pub fn remove_node(&mut self, node_id: &str) {
        for state in &mut self.states {
            state.remove(node_id);
        }
    }

    /// Selects an index for the given bindings/request and walks its shard.
    /// `budget` truncates the ordered candidates; `None` returns the whole
    /// shard (the equivalence-preserving mode).
    pub fn query(
        &self,
        bindings: &HashMap<String, Value>,
        request: &HashMap<String, f64>,
        budget: Option<usize>,
    ) -> IndexQueryOutcome {
        if self.settings.indexes.is_empty() || self.settings.min_nodes == 0 {
            return IndexQueryOutcome::Disabled;
        }
        let idx = match self.settings.select(bindings, request) {
            Ok(idx) => idx,
            Err(reason) => return IndexQueryOutcome::Unusable { reason },
        };
        let spec = &self.settings.indexes[idx];
        let state = &self.states[idx];

        let partition = spec
            .partition_by
            .iter()
            .map(|key| {
                bindings
                    .get(key)
                    .map(value_token)
                    // select() guarantees the binding exists; stay defensive.
                    .unwrap_or_else(|| ABSENT_TOKEN.to_string())
            })
            .collect::<Vec<_>>()
            .join(&PARTITION_SEPARATOR.to_string());

        let Some(shard) = state.shards.get(&partition) else {
            // No node carries this exact attribute tuple. The scan would
            // reject every node on the same equality constraint, so an empty
            // candidate set is the *correct* answer, not a fallback case.
            return IndexQueryOutcome::Candidates(IndexCandidates {
                index: spec.name.clone(),
                mode: spec.mode,
                node_ids: Vec::new(),
                partition_size: 0,
                truncated: false,
            });
        };

        let request_cell: Vec<u16> = spec
            .dims
            .iter()
            .map(|dim| {
                let wanted = dim
                    .attr
                    .strip_prefix(RESOURCE_DIM_PREFIX)
                    .and_then(|name| request.get(name).copied())
                    .unwrap_or(0.0);
                bucket_value(&dim.scale, wanted)
            })
            .collect();

        // Order every occupied cell: the orthant (every bucket covers the
        // request's) first by weighted distance — ascending for tight,
        // descending for loose — then the provably-non-fitting tail, closest
        // first. Cell key breaks ties so output is deterministic.
        let mut cells: Vec<(&Vec<u16>, &Vec<String>)> = shard.cells.iter().collect();
        let ranked = |cell: &[u16]| -> (u8, f64) {
            let fits = cell
                .iter()
                .zip(request_cell.iter())
                .all(|(have, want)| have >= want);
            let distance: f64 = cell
                .iter()
                .zip(request_cell.iter())
                .zip(spec.dims.iter())
                .map(|((have, want), dim)| dim.weight * (f64::from(*have) - f64::from(*want)).abs())
                .sum();
            if fits {
                let distance = match spec.order {
                    IndexOrder::Tight => distance,
                    IndexOrder::Loose => -distance,
                };
                (0, distance)
            } else {
                (1, distance)
            }
        };
        cells.sort_by(|a, b| {
            let (fit_a, dist_a) = ranked(a.0);
            let (fit_b, dist_b) = ranked(b.0);
            fit_a
                .cmp(&fit_b)
                .then(dist_a.total_cmp(&dist_b))
                .then(a.0.cmp(b.0))
        });

        let partition_size: usize = cells.iter().map(|(_, ids)| ids.len()).sum();
        let mut node_ids = Vec::with_capacity(budget.unwrap_or(partition_size).min(partition_size));
        'outer: for (_, ids) in &cells {
            for id in ids.iter() {
                if let Some(budget) = budget {
                    if node_ids.len() >= budget {
                        break 'outer;
                    }
                }
                node_ids.push(id.clone());
            }
        }
        let truncated = node_ids.len() < partition_size;

        IndexQueryOutcome::Candidates(IndexCandidates {
            index: spec.name.clone(),
            mode: spec.mode,
            node_ids,
            partition_size,
            truncated,
        })
    }
}

fn project_node(spec: &IndexSpec, node: &Node, free: &HashMap<String, f64>) -> NodeProjection {
    let partition = spec
        .partition_by
        .iter()
        .map(|key| partition_token(node, key))
        .collect::<Vec<_>>()
        .join(&PARTITION_SEPARATOR.to_string());
    let cell = spec
        .dims
        .iter()
        .map(|dim| bucket_value(&dim.scale, dim_value(node, free, dim)))
        .collect();
    (partition, cell)
}

/// `pool` reads the node's pool column (the same special case permits use);
/// everything else reads the attribute chain.
fn partition_token(node: &Node, key: &str) -> String {
    if key == "pool" {
        return value_token(&Value::String(node.pool.clone()));
    }
    node_attr(node, key)
        .map(value_token)
        .unwrap_or_else(|| ABSENT_TOKEN.to_string())
}

/// A missing or non-numeric dim value projects to zero — the node matches
/// requests that don't ask for the dim and sorts last for ones that do.
fn dim_value(node: &Node, free: &HashMap<String, f64>, dim: &DimSpec) -> f64 {
    if let Some(resource) = dim.attr.strip_prefix(RESOURCE_DIM_PREFIX) {
        return free.get(resource).copied().unwrap_or(0.0);
    }
    node_attr(node, &dim.attr)
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

fn bucket_value(scale: &DimScale, value: f64) -> u16 {
    // NaN (a non-numeric or unparsable attr) buckets as zero, same as
    // missing: the node matches requests that don't ask for the dim and
    // sorts last for ones that do.
    if value.is_nan() || value <= 0.0 {
        return 0;
    }
    match scale {
        DimScale::Log => {
            if value.is_infinite() {
                LOG_INFINITY_BUCKET
            } else {
                ((value + 1.0).log2().floor() as u16).min(LOG_INFINITY_BUCKET - 1)
            }
        }
        DimScale::Linear { step } => {
            if value.is_infinite() {
                u16::MAX
            } else {
                (value / step).floor().min(f64::from(u16::MAX)) as u16
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_node(id: &str, pool: &str, config_attrs: Value) -> Node {
        Node {
            id: id.into(),
            status: "active".into(),
            pool: pool.into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
            config_attrs,
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn settings(value: Value) -> IndexSettings {
        IndexSettings::from_scheduler_settings(Some(&value), "bin_pack").expect("settings")
    }

    fn set_for(
        value: Value,
        nodes: Vec<Node>,
    ) -> (
        IndexSet,
        HashMap<String, Node>,
        HashMap<String, Vec<Allocation>>,
    ) {
        let parsed = Arc::new(settings(value));
        let nodes: HashMap<String, Node> = nodes
            .into_iter()
            .map(|node| (node.id.clone(), node))
            .collect();
        let allocations = HashMap::new();
        let policy = ResourcePolicy::from_manager_config(&Default::default()).expect("policy");
        let set = IndexSet::build(parsed, &nodes, &allocations, &policy);
        (set, nodes, allocations)
    }

    #[test]
    fn default_settings_include_the_fit_index() {
        let parsed = IndexSettings::from_scheduler_settings(None, "bin_pack").expect("settings");
        assert_eq!(parsed.indexes.len(), 1);
        let fit = &parsed.indexes[0];
        assert_eq!(fit.name, "fit");
        assert_eq!(fit.partition_by, vec!["pool"]);
        assert_eq!(fit.dims.len(), 3);
        assert_eq!(fit.order, IndexOrder::Tight);
        assert_eq!(fit.mode, IndexMode::On);
        assert_eq!(parsed.min_nodes, DEFAULT_INDEX_MIN_NODES);
    }

    #[test]
    fn spread_policy_defaults_to_loose_order() {
        let parsed = IndexSettings::from_scheduler_settings(None, "spread").expect("settings");
        assert_eq!(parsed.indexes[0].order, IndexOrder::Loose);
    }

    #[test]
    fn fit_false_disables_the_default_index() {
        let parsed = settings(json!({ "indexes": { "fit": false } }));
        assert!(parsed.indexes.is_empty());
        assert!(!parsed.any_active());
    }

    #[test]
    fn shorthand_splits_partitions_and_dims() {
        let parsed = settings(json!({
            "indexes": { "gpu": ["pool", "region", "resources.gpu_vram"], "fit": false }
        }));
        assert_eq!(parsed.indexes.len(), 1);
        let gpu = &parsed.indexes[0];
        assert_eq!(gpu.partition_by, vec!["pool", "region"]);
        assert_eq!(gpu.dims.len(), 1);
        assert_eq!(gpu.dims[0].attr, "resources.gpu_vram");
    }

    #[test]
    fn full_form_parses_order_mode_and_dim_options() {
        let parsed = settings(json!({
            "indexes": {
                "fit": false,
                "big": {
                    "partition_by": ["pool"],
                    "dims": [
                        { "attr": "resources.memory", "weight": 2 },
                        { "attr": "gpu_count", "scale": "linear", "step": 1 },
                    ],
                    "order": "loose",
                    "mode": "shadow",
                },
            },
        }));
        let big = &parsed.indexes[0];
        assert_eq!(big.order, IndexOrder::Loose);
        assert_eq!(big.mode, IndexMode::Shadow);
        assert_eq!(big.dims[0].weight, 2.0);
        assert_eq!(big.dims[1].scale, DimScale::Linear { step: 1.0 });
    }

    #[test]
    fn user_fit_overrides_the_default() {
        let parsed = settings(json!({
            "indexes": { "fit": { "partition_by": ["pool"], "dims": ["resources.memory"] } }
        }));
        assert_eq!(parsed.indexes.len(), 1);
        assert_eq!(parsed.indexes[0].dims.len(), 1);
    }

    #[test]
    fn parse_rejects_bad_specs() {
        for (case, value) in [
            (
                "unknown field",
                json!({ "indexes": { "x": { "partition": ["a"] } } }),
            ),
            ("empty spec", json!({ "indexes": { "x": {} } })),
            (
                "too many dims",
                json!({ "indexes": { "x": { "dims": [
                "resources.a", "resources.b", "resources.c", "resources.d",
                "resources.e", "resources.f", "resources.g"] } } }),
            ),
            (
                "bad order",
                json!({ "indexes": { "x": { "partition_by": ["a"], "order": "best" } } }),
            ),
            (
                "bad mode",
                json!({ "indexes": { "x": { "partition_by": ["a"], "mode": "maybe" } } }),
            ),
            ("true non-fit", json!({ "indexes": { "x": true } })),
            (
                "negative weight",
                json!({ "indexes": { "x": { "dims": [{ "attr": "resources.a", "weight": -1 }] } } }),
            ),
            (
                "step on log",
                json!({ "indexes": { "x": { "dims": [{ "attr": "resources.a", "step": 2 }] } } }),
            ),
            (
                "zero step",
                json!({ "indexes": { "x": { "dims": [{ "attr": "a", "scale": "linear", "step": 0 }] } } }),
            ),
            (
                "duplicate attr",
                json!({ "indexes": { "x": ["region", "region"] } }),
            ),
            ("non-object indexes", json!({ "indexes": ["region"] })),
        ] {
            let result = IndexSettings::from_scheduler_settings(Some(&value), "bin_pack");
            assert!(result.is_err(), "{case} should fail to parse");
        }
    }

    #[test]
    fn log_buckets_are_monotone_and_handle_edges() {
        let log = DimScale::Log;
        assert_eq!(bucket_value(&log, -5.0), 0);
        assert_eq!(bucket_value(&log, 0.0), 0);
        assert_eq!(bucket_value(&log, 1.0), 1);
        assert_eq!(bucket_value(&log, 1023.0), 10);
        assert_eq!(bucket_value(&log, f64::NAN), 0);
        assert_eq!(bucket_value(&log, f64::INFINITY), LOG_INFINITY_BUCKET);
        let mut last = 0;
        for value in [0.0, 0.5, 1.0, 3.0, 10.0, 100.0, 1e6, 1e12, 1e30] {
            let bucket = bucket_value(&log, value);
            assert!(bucket >= last, "log buckets must be monotone");
            last = bucket;
        }
    }

    #[test]
    fn linear_buckets_step_and_saturate() {
        let linear = DimScale::Linear { step: 2.0 };
        assert_eq!(bucket_value(&linear, 0.0), 0);
        assert_eq!(bucket_value(&linear, 1.9), 0);
        assert_eq!(bucket_value(&linear, 2.0), 1);
        assert_eq!(bucket_value(&linear, 9.0), 4);
        assert_eq!(bucket_value(&linear, f64::INFINITY), u16::MAX);
        assert_eq!(bucket_value(&linear, 1e12), u16::MAX);
    }

    #[test]
    fn partition_query_returns_exactly_the_matching_nodes() {
        let (set, _, _) = set_for(
            json!({ "indexes": { "fit": false, "topo": ["region"] } }),
            vec![
                make_node("a", "default", json!({ "region": "east" })),
                make_node("b", "default", json!({ "region": "west" })),
                make_node("c", "default", json!({ "region": "east" })),
                make_node("d", "default", json!({})),
            ],
        );
        let bindings = HashMap::from([("region".to_string(), json!("east"))]);
        let outcome = set.query(&bindings, &HashMap::new(), None);
        let IndexQueryOutcome::Candidates(candidates) = outcome else {
            panic!("expected candidates");
        };
        assert_eq!(candidates.node_ids, vec!["a", "c"]);
        assert_eq!(candidates.partition_size, 2);
        assert!(!candidates.truncated);
    }

    #[test]
    fn missing_partition_value_matches_no_bound_query() {
        let (set, _, _) = set_for(
            json!({ "indexes": { "fit": false, "topo": ["region"] } }),
            vec![make_node("a", "default", json!({}))],
        );
        let bindings = HashMap::from([("region".to_string(), json!("east"))]);
        let IndexQueryOutcome::Candidates(candidates) = set.query(&bindings, &HashMap::new(), None)
        else {
            panic!("expected candidates");
        };
        assert!(candidates.node_ids.is_empty());
    }

    #[test]
    fn unbound_partition_key_reports_unusable() {
        let (set, _, _) = set_for(
            json!({ "indexes": { "fit": false, "topo": ["region", "zone"] } }),
            vec![make_node(
                "a",
                "default",
                json!({ "region": "east", "zone": "1" }),
            )],
        );
        let bindings = HashMap::from([("region".to_string(), json!("east"))]);
        let outcome = set.query(&bindings, &HashMap::new(), None);
        let IndexQueryOutcome::Unusable { reason } = outcome else {
            panic!("expected unusable");
        };
        assert!(reason.contains("topo needs [zone]"), "{reason}");
    }

    #[test]
    fn selection_prefers_request_coverage_then_partitions() {
        let parsed = settings(json!({
            "indexes": {
                "fit": false,
                "topo": ["pool", "region"],
                "gpu": ["pool", "resources.gpu_vram"],
            }
        }));
        let bindings = HashMap::from([
            ("pool".to_string(), json!("default")),
            ("region".to_string(), json!("east")),
        ]);
        // A gpu_vram request: coverage wins over partition count.
        let request = HashMap::from([("gpu_vram".to_string(), 1024.0)]);
        let idx = parsed.select(&bindings, &request).expect("selects");
        assert_eq!(parsed.indexes[idx].name, "gpu");
        // No gpu request: equal coverage (0), more partition keys wins.
        let idx = parsed.select(&bindings, &HashMap::new()).expect("selects");
        assert_eq!(parsed.indexes[idx].name, "topo");
    }

    #[test]
    fn off_mode_indexes_are_never_selected() {
        let parsed = settings(json!({
            "indexes": {
                "fit": false,
                "topo": { "partition_by": ["region"], "mode": "off" },
            }
        }));
        let bindings = HashMap::from([("region".to_string(), json!("east"))]);
        assert!(parsed.select(&bindings, &HashMap::new()).is_err());
        assert!(!parsed.any_active());
    }

    fn fit_nodes() -> Vec<Node> {
        // Free memory comes from fingerprints; give each node a distinct
        // total via config_attrs custom resources instead (simpler to
        // control), using a custom resource dim.
        vec![
            make_node("small", "default", json!({ "resources": { "vram": 8 } })),
            make_node(
                "medium",
                "default",
                json!({ "resources": { "vram": 1024 } }),
            ),
            make_node(
                "large",
                "default",
                json!({ "resources": { "vram": 65536 } }),
            ),
            make_node("none", "default", json!({})),
        ]
    }

    #[test]
    fn tight_order_prefers_smallest_sufficient_fit() {
        let (set, _, _) = set_for(
            json!({ "indexes": { "fit": false, "vram": ["pool", "resources.vram"] } }),
            fit_nodes(),
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = HashMap::from([("vram".to_string(), 512.0)]);
        let IndexQueryOutcome::Candidates(candidates) = set.query(&bindings, &request, None) else {
            panic!("expected candidates");
        };
        // Fitting nodes nearest-first, then the provably-too-small tail.
        assert_eq!(
            candidates.node_ids,
            vec!["medium", "large", "small", "none"]
        );
        assert_eq!(candidates.partition_size, 4);
    }

    #[test]
    fn loose_order_prefers_most_headroom() {
        let (set, _, _) = set_for(
            json!({ "indexes": { "fit": false, "vram": {
                "partition_by": ["pool"], "dims": ["resources.vram"], "order": "loose",
            } } }),
            fit_nodes(),
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = HashMap::from([("vram".to_string(), 512.0)]);
        let IndexQueryOutcome::Candidates(candidates) = set.query(&bindings, &request, None) else {
            panic!("expected candidates");
        };
        assert_eq!(
            candidates.node_ids,
            vec!["large", "medium", "small", "none"]
        );
    }

    #[test]
    fn budget_truncates_but_never_hides_the_shard_size() {
        let (set, _, _) = set_for(
            json!({ "indexes": { "fit": false, "vram": ["pool", "resources.vram"] } }),
            fit_nodes(),
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = HashMap::from([("vram".to_string(), 512.0)]);
        let IndexQueryOutcome::Candidates(candidates) = set.query(&bindings, &request, Some(2))
        else {
            panic!("expected candidates");
        };
        assert_eq!(candidates.node_ids, vec!["medium", "large"]);
        assert_eq!(candidates.partition_size, 4);
        assert!(candidates.truncated);
    }

    #[test]
    fn zero_request_matches_zero_free_nodes() {
        let (set, _, _) = set_for(
            json!({ "indexes": { "fit": false, "vram": ["pool", "resources.vram"] } }),
            fit_nodes(),
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        // No vram in the request: every node fits, including "none".
        let IndexQueryOutcome::Candidates(candidates) = set.query(&bindings, &HashMap::new(), None)
        else {
            panic!("expected candidates");
        };
        assert_eq!(candidates.node_ids.len(), 4);
        assert_eq!(candidates.node_ids[0], "none");
    }

    #[test]
    fn reindex_moves_updates_and_removes_nodes() {
        let (mut set, mut nodes, allocations) = set_for(
            json!({ "indexes": { "fit": false, "topo": ["region"] } }),
            vec![make_node("a", "default", json!({ "region": "east" }))],
        );
        let policy = ResourcePolicy::from_manager_config(&Default::default()).expect("policy");
        let east = HashMap::from([("region".to_string(), json!("east"))]);
        let west = HashMap::from([("region".to_string(), json!("west"))]);

        // Move a to west.
        nodes.get_mut("a").unwrap().config_attrs = json!({ "region": "west" });
        set.reindex_node("a", &nodes, &allocations, &policy);
        let IndexQueryOutcome::Candidates(candidates) = set.query(&east, &HashMap::new(), None)
        else {
            panic!()
        };
        assert!(candidates.node_ids.is_empty());
        let IndexQueryOutcome::Candidates(candidates) = set.query(&west, &HashMap::new(), None)
        else {
            panic!()
        };
        assert_eq!(candidates.node_ids, vec!["a"]);

        // Unavailable nodes leave the index.
        nodes.get_mut("a").unwrap().availability = "unavailable".into();
        set.reindex_node("a", &nodes, &allocations, &policy);
        let IndexQueryOutcome::Candidates(candidates) = set.query(&west, &HashMap::new(), None)
        else {
            panic!()
        };
        assert!(candidates.node_ids.is_empty());

        // And removal is idempotent.
        set.remove_node("a");
        set.remove_node("a");
    }

    #[test]
    fn min_nodes_zero_disables_queries() {
        let (set, _, _) = set_for(
            json!({ "index_min_nodes": 0, "indexes": { "topo": ["region"] } }),
            vec![make_node("a", "default", json!({ "region": "east" }))],
        );
        let bindings = HashMap::from([("region".to_string(), json!("east"))]);
        assert!(matches!(
            set.query(&bindings, &HashMap::new(), None),
            IndexQueryOutcome::Disabled
        ));
    }

    #[test]
    fn pool_partition_reads_the_pool_column() {
        let (set, _, _) = set_for(
            json!({ "indexes": { "fit": false, "pools": ["pool"] } }),
            vec![
                make_node("a", "gpu-pool", json!({})),
                make_node("b", "default", json!({})),
            ],
        );
        let bindings = HashMap::from([("pool".to_string(), json!("gpu-pool"))]);
        let IndexQueryOutcome::Candidates(candidates) = set.query(&bindings, &HashMap::new(), None)
        else {
            panic!()
        };
        assert_eq!(candidates.node_ids, vec!["a"]);
    }
}
