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
//! Within one cell the index has no preference, so when a budget cut lands
//! inside a cell the remainder is sampled uniformly from that cell rather
//! than taken as its sorted-id prefix. A prefix would make every planner's
//! candidate universe the same lowest-id nodes of the boundary cell, which is
//! how a `spread` workload once concentrated on one quarter of a fleet
//! (`internal_docs/aws-phase5-findings.md`).
//!
//! The index only ever *proposes* candidates. Every consumer re-verifies them
//! through the ordinary constraint/permit/ledger chain, so a stale or wrong
//! index costs a refill or a worse pick, never an invalid placement.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::constraints::{node_attr, value_token};
use crate::prelude::*;
use crate::resources::ResourcePool;
use crate::structs::Node;

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
    /// Parallel to `node_ids`: the rank of the cell each id came from in
    /// the walk (0 = best-fitting). Nodes sharing a rank are
    /// indistinguishable to the index, which is what a caller that
    /// truncates AFTER re-verification needs to know — it must sample the
    /// boundary cell rather than cut it at the budget
    /// ([`truncate_within_boundary_cell`]). Empty for an unordered query.
    pub cell_ranks: Vec<u32>,
    /// Shard population before any budget truncation.
    pub partition_size: usize,
    pub truncated: bool,
}

/// Cut a best-first list of `(item, cell rank)` down to `budget` items.
///
/// Every rank that fits entirely is kept whole and in order. The rank the
/// cut lands in is sampled uniformly at random for the remaining slots —
/// never taken as a prefix — because the index has no preference between
/// nodes of one cell, and a prefix would give every plan the same few
/// nodes of that cell (the lowest ids: the failure recorded in
/// `internal_docs/aws-phase5-findings.md`). Output keeps input order.
pub fn truncate_within_boundary_cell<T>(ranked: Vec<(T, u32)>, budget: usize) -> Vec<T> {
    if ranked.len() <= budget {
        return ranked.into_iter().map(|(item, _)| item).collect();
    }
    // Find the boundary rank: the first rank whose end passes the budget.
    let mut whole_end = 0usize; // items before the boundary rank
    let mut boundary_end = ranked.len();
    let mut index = 0usize;
    while index < ranked.len() {
        let rank = ranked[index].1;
        let mut end = index;
        while end < ranked.len() && ranked[end].1 == rank {
            end += 1;
        }
        if end > budget {
            whole_end = index;
            boundary_end = end;
            break;
        }
        index = end;
    }
    let remaining = budget - whole_end;
    let mut picks =
        rand::seq::index::sample(&mut rand::thread_rng(), boundary_end - whole_end, remaining)
            .into_vec();
    picks.sort_unstable();
    let mut picks = picks.into_iter().peekable();
    let mut out = Vec::with_capacity(budget);
    for (position, (item, _)) in ranked.into_iter().enumerate() {
        if position < whole_end {
            out.push(item);
        } else if position < boundary_end {
            if picks.peek() == Some(&(position - whole_end)) {
                picks.next();
                out.push(item);
            }
        } else {
            break;
        }
    }
    out
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

/// What [`IndexSet::walk_verified`] produced: at most `budget` verified
/// candidates, best-first when `truncated`, in walk order otherwise (the
/// caller re-sorts that case by node id).
#[derive(Debug)]
pub struct IndexWalk<T> {
    pub index: String,
    pub items: Vec<T>,
    /// Shard population, verified or not.
    pub partition_size: usize,
    /// More eligible nodes existed than `budget` (the old
    /// `verified.len() > budget`).
    pub truncated: bool,
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
    pub fn new(settings: Arc<IndexSettings>) -> IndexSet {
        IndexSet {
            states: settings
                .indexes
                .iter()
                .map(|_| IndexState::default())
                .collect(),
            settings,
        }
    }

    pub fn settings(&self) -> &IndexSettings {
        &self.settings
    }

    /// True when at least one index can serve queries at all (before the
    /// per-plan `min_nodes` arming check, which needs the candidate count).
    pub fn any_active(&self) -> bool {
        self.settings.any_active()
    }

    /// Re-projects one **available** node into every index, reading free
    /// resource capacity from the read model's cached pool (the same pool
    /// the plan-time ledger clones, so the two can never disagree). Skips
    /// shard surgery when the projection is unchanged (the common heartbeat
    /// case). Callers remove unavailable or deleted nodes via
    /// [`IndexSet::remove_node`] instead.
    pub(crate) fn reindex_node(&mut self, node: &Node, pool: &ResourcePool) {
        if self.settings.indexes.is_empty() {
            return;
        }
        for (spec, state) in self.settings.indexes.iter().zip(self.states.iter_mut()) {
            let projection = project_node(spec, node, pool);
            if state.node_records.get(node.id.as_str()) == Some(&projection) {
                continue;
            }
            state.remove(&node.id);
            state.insert(&node.id, projection);
        }
    }

    pub fn remove_node(&mut self, node_id: &str) {
        for state in &mut self.states {
            state.remove(node_id);
        }
    }

    /// Selects an index for the given bindings/request and walks its shard.
    /// `budget` truncates the ordered candidates; `None` returns the whole
    /// shard (the equivalence-preserving mode). Candidates come back
    /// best-first per the index's order.
    pub fn query(
        &self,
        bindings: &HashMap<String, Value>,
        request: &HashMap<String, f64>,
        budget: Option<usize>,
    ) -> IndexQueryOutcome {
        self.query_with_order(bindings, request, budget, true)
    }

    /// [`IndexSet::query`], with the nearest-fit cell ordering optional.
    /// A caller that re-orders the candidates itself (the scheduler's
    /// untruncated path sorts verified candidates by node id) passes
    /// `ordered: false` and skips the shard's cell sort entirely — under
    /// `budget: None` the sort's only effect was an ordering the caller
    /// threw away.
    /// The index this query uses and its shard's occupied cells, walked
    /// best-first when `ordered` (the cell rank is the position here).
    /// `Err` carries an outcome that needs no walk: indexing disabled, no
    /// usable index, or an empty partition (an exact, empty answer).
    fn select_cells<'s>(
        &'s self,
        bindings: &HashMap<String, Value>,
        request: &HashMap<String, f64>,
        ordered: bool,
    ) -> Result<(&'s IndexSpec, Vec<&'s Vec<String>>), IndexQueryOutcome> {
        if self.settings.indexes.is_empty() || self.settings.min_nodes == 0 {
            return Err(IndexQueryOutcome::Disabled);
        }
        let idx = match self.settings.select(bindings, request) {
            Ok(idx) => idx,
            Err(reason) => return Err(IndexQueryOutcome::Unusable { reason }),
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
            return Err(IndexQueryOutcome::Candidates(IndexCandidates {
                index: spec.name.clone(),
                mode: spec.mode,
                node_ids: Vec::new(),
                cell_ranks: Vec::new(),
                partition_size: 0,
                truncated: false,
            }));
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
        // first. Cell key breaks ties between cells so the cell walk is
        // deterministic. Skipped entirely for unordered queries (the caller
        // re-orders).
        let mut cells: Vec<(&Vec<u16>, &Vec<String>)> = shard.cells.iter().collect();
        if ordered {
            let ranked = |cell: &[u16]| -> (u8, f64) {
                let fits = cell
                    .iter()
                    .zip(request_cell.iter())
                    .all(|(have, want)| have >= want);
                let distance: f64 = cell
                    .iter()
                    .zip(request_cell.iter())
                    .zip(spec.dims.iter())
                    .map(|((have, want), dim)| {
                        dim.weight * (f64::from(*have) - f64::from(*want)).abs()
                    })
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
            // Rank once per cell, not once per comparison: the comparator
            // used to recompute both sides' ranks O(n log n) times.
            type RankedCell<'c> = ((u8, f64), (&'c Vec<u16>, &'c Vec<String>));
            let mut ranked_cells: Vec<RankedCell<'_>> = cells
                .into_iter()
                .map(|cell| (ranked(cell.0), cell))
                .collect();
            ranked_cells.sort_by(|a, b| {
                (a.0 .0)
                    .cmp(&b.0 .0)
                    .then(a.0 .1.total_cmp(&b.0 .1))
                    .then(a.1 .0.cmp(b.1 .0))
            });
            cells = ranked_cells.into_iter().map(|(_, cell)| cell).collect();
        }

        Ok((spec, cells.into_iter().map(|(_, ids)| ids).collect()))
    }

    pub fn query_with_order(
        &self,
        bindings: &HashMap<String, Value>,
        request: &HashMap<String, f64>,
        budget: Option<usize>,
        ordered: bool,
    ) -> IndexQueryOutcome {
        let (spec, cells) = match self.select_cells(bindings, request, ordered) {
            Ok(selected) => selected,
            Err(outcome) => return outcome,
        };
        let partition_size: usize = cells.iter().map(|ids| ids.len()).sum();
        // Tag every id with its cell's rank in the walk. Within a cell the
        // index has no opinion, so a budget cut that lands inside a cell
        // samples it (`truncate_within_boundary_cell`) — the sorted-id
        // prefix it used to take is what packed a 5,000-node fleet onto
        // its lexicographically-first 1,250 nodes (aws-phase5-findings.md).
        // The scheduler asks for the whole shard and truncates AFTER
        // re-verification, so the ranks travel with the ids.
        let mut ranked: Vec<(String, u32)> = Vec::with_capacity(partition_size);
        for (rank, ids) in cells.iter().enumerate() {
            ranked.extend(ids.iter().map(|id| (id.clone(), rank as u32)));
        }
        let (node_ids, cell_ranks): (Vec<String>, Vec<u32>) = match budget {
            Some(budget) if budget < ranked.len() => truncate_within_boundary_cell(
                ranked
                    .into_iter()
                    .map(|(id, rank)| ((id, rank), rank))
                    .collect(),
                budget,
            )
            .into_iter()
            .unzip(),
            _ => ranked.into_iter().unzip(),
        };
        // Unordered queries carry no meaningful ranks (the walk was not
        // sorted), and the caller re-orders anyway.
        let cell_ranks = if ordered { cell_ranks } else { Vec::new() };
        let truncated = node_ids.len() < partition_size;

        IndexQueryOutcome::Candidates(IndexCandidates {
            index: spec.name.clone(),
            mode: spec.mode,
            node_ids,
            cell_ranks,
            partition_size,
            truncated,
        })
    }

    /// Budgeted, verify-as-you-walk candidates (plan-path-fleet-scaling
    /// plan § 6.2): the same output the scheduler used to get from
    /// `query_with_order(.., None, true)` + verifying every id +
    /// [`truncate_within_boundary_cell`], without materializing or verifying
    /// the whole shard. The default `fit` index partitions by pool only, so
    /// on a one-pool fleet the shard IS the fleet, and the old path cloned
    /// and verified every node id per task — linear in fleet size under the
    /// read-model lock.
    ///
    /// Cells are walked best-first. A cell that fits in the remaining
    /// budget is verified whole and kept in order. The first cell that does
    /// not fit is drawn in uniformly random order and verified one member
    /// at a time: the first `remaining` verified members are a uniform
    /// sample of the cell's verified members, which is exactly what
    /// `truncate_within_boundary_cell` takes from the boundary rank, and
    /// they are emitted in the cell's own order, as it emits them. If the
    /// cell runs out first, all its verified members were kept and the walk
    /// moves on.
    ///
    /// `truncated` mirrors the old `verified.len() > budget` exactly: after
    /// the budget is full the walk keeps verifying until it finds ONE more
    /// eligible node (usually immediately) or runs out. A caller must
    /// re-sort an untruncated result by node id, as before.
    ///
    /// Returns `Err` with the plain query's outcome when the walk does not
    /// apply: indexing disabled, no usable index, an empty partition, or an
    /// index not in `on` mode (shadow mode compares the full candidate set
    /// with the scan, so it keeps the full query).
    pub fn walk_verified<T>(
        &self,
        bindings: &HashMap<String, Value>,
        request: &HashMap<String, f64>,
        budget: usize,
        mut verify: impl FnMut(&str) -> Option<T>,
    ) -> Result<IndexWalk<T>, IndexQueryOutcome> {
        use rand::Rng;

        let (spec, cells) = self.select_cells(bindings, request, true)?;
        if spec.mode != IndexMode::On {
            return Err(self.query_with_order(bindings, request, None, true));
        }
        let partition_size: usize = cells.iter().map(|ids| ids.len()).sum();
        let mut rng = rand::thread_rng();
        let mut out: Vec<T> = Vec::with_capacity(budget.min(partition_size));
        let mut truncated = false;
        for ids in cells {
            let remaining = budget - out.len();
            if remaining == 0 {
                // Budget full: one more eligible node anywhere means the
                // old path would have truncated (and kept exactly `out`).
                if ids.iter().any(|id| verify(id).is_some()) {
                    truncated = true;
                    break;
                }
                continue;
            }
            if ids.len() <= remaining {
                out.extend(ids.iter().filter_map(|id| verify(id)));
                continue;
            }
            // The boundary candidate: a lazy Fisher-Yates over positions,
            // stopping one verified member past `remaining`.
            let mut order: Vec<u32> = (0..ids.len() as u32).collect();
            let mut accepted: Vec<(u32, T)> = Vec::with_capacity(remaining);
            let mut extra = false;
            for i in 0..order.len() {
                let j = rng.gen_range(i..order.len());
                order.swap(i, j);
                let position = order[i];
                if let Some(item) = verify(&ids[position as usize]) {
                    if accepted.len() < remaining {
                        accepted.push((position, item));
                    } else {
                        extra = true;
                        break;
                    }
                }
            }
            accepted.sort_unstable_by_key(|(position, _)| *position);
            out.extend(accepted.into_iter().map(|(_, item)| item));
            if extra {
                truncated = true;
                break;
            }
        }
        Ok(IndexWalk {
            index: spec.name.clone(),
            items: out,
            partition_size,
            truncated,
        })
    }
}

fn project_node(spec: &IndexSpec, node: &Node, pool: &ResourcePool) -> NodeProjection {
    let partition = spec
        .partition_by
        .iter()
        .map(|key| partition_token(node, key))
        .collect::<Vec<_>>()
        .join(&PARTITION_SEPARATOR.to_string());
    let cell = spec
        .dims
        .iter()
        .map(|dim| bucket_value(&dim.scale, dim_value(node, pool, dim)))
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
fn dim_value(node: &Node, pool: &ResourcePool, dim: &DimSpec) -> f64 {
    if let Some(resource) = dim.attr.strip_prefix(RESOURCE_DIM_PREFIX) {
        return pool.amount(resource).unwrap_or(0.0);
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
            heartbeat_source: String::new(),
            config_attrs,
            fingerprint_attrs: json!({}),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn settings(value: Value) -> IndexSettings {
        IndexSettings::from_scheduler_settings(Some(&value), "bin_pack").expect("settings")
    }

    /// Free pool as the read model would compute it (no allocations here).
    fn pool_for(node: &Node) -> ResourcePool {
        let policy = crate::resources::ResourcePolicy::from_manager_config(&Default::default())
            .expect("policy");
        ResourcePool::free_for_node(node, &[], &policy)
    }

    fn set_for(value: Value, nodes: Vec<Node>) -> (IndexSet, HashMap<String, Node>) {
        let parsed = Arc::new(settings(value));
        let nodes: HashMap<String, Node> = nodes
            .into_iter()
            .map(|node| (node.id.clone(), node))
            .collect();
        let mut set = IndexSet::new(parsed);
        for node in nodes.values() {
            if node.availability == "available" {
                set.reindex_node(node, &pool_for(node));
            }
        }
        (set, nodes)
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
        let (set, _) = set_for(
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
        let (set, _) = set_for(
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
        let (set, _) = set_for(
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
        let (set, _) = set_for(
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
        let (set, _) = set_for(
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

    /// An unordered query is the ordered query minus the cell sort: same
    /// candidate SET, same partition size, no truncation — only the
    /// best-first ordering is dropped (the scheduler's untruncated path
    /// re-sorts by node id anyway).
    #[test]
    fn unordered_query_returns_the_same_candidate_set() {
        let (set, _) = set_for(
            json!({ "indexes": { "fit": false, "vram": ["pool", "resources.vram"] } }),
            fit_nodes(),
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = HashMap::from([("vram".to_string(), 512.0)]);
        let IndexQueryOutcome::Candidates(ordered) = set.query(&bindings, &request, None) else {
            panic!("expected candidates");
        };
        let IndexQueryOutcome::Candidates(unordered) =
            set.query_with_order(&bindings, &request, None, false)
        else {
            panic!("expected candidates");
        };
        let mut ordered_ids = ordered.node_ids.clone();
        let mut unordered_ids = unordered.node_ids.clone();
        ordered_ids.sort();
        unordered_ids.sort();
        assert_eq!(ordered_ids, unordered_ids);
        assert_eq!(unordered.partition_size, ordered.partition_size);
        assert!(!unordered.truncated);
    }

    #[test]
    fn budget_truncates_but_never_hides_the_shard_size() {
        let (set, _) = set_for(
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

    /// The rig's shape in miniature: many identical nodes in ONE cell and a
    /// budget far below the cell. The cut must not be the sorted-id prefix,
    /// and repeated queries must reach past it.
    #[test]
    fn budget_cut_inside_a_tied_cell_samples_rather_than_prefixes() {
        let nodes: Vec<Node> = (0..4)
            .flat_map(|slice| {
                (0..250).map(move |n| {
                    make_node(
                        &format!("sim-{slice}-web-{n:03}"),
                        "default",
                        json!({ "resources": { "vram": 1024 } }),
                    )
                })
            })
            .collect();
        let (set, _) = set_for(
            json!({ "indexes": { "fit": false, "vram": ["pool", "resources.vram"] } }),
            nodes,
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = HashMap::from([("vram".to_string(), 512.0)]);

        let mut seen: HashSet<String> = HashSet::new();
        for _ in 0..20 {
            let IndexQueryOutcome::Candidates(candidates) =
                set.query(&bindings, &request, Some(64))
            else {
                panic!("expected candidates");
            };
            assert_eq!(candidates.node_ids.len(), 64);
            assert_eq!(candidates.partition_size, 1000);
            assert!(candidates.truncated);
            // Not the prefix: a uniform 64-of-1000 sample lands entirely in
            // slice 0 (the first 250 ids) with probability ~(1/4)^64.
            let slice0 = candidates
                .node_ids
                .iter()
                .filter(|id| id.starts_with("sim-0-"))
                .count();
            assert!(
                slice0 < 64,
                "candidates were the sorted-id prefix: {:?}",
                candidates.node_ids
            );
            seen.extend(candidates.node_ids.iter().cloned());
        }
        // 20 draws of 64 from 1,000 reach far beyond any single prefix.
        assert!(
            seen.len() > 400,
            "queries kept returning the same nodes: {}",
            seen.len()
        );
    }

    // --- walk_verified (plan-path-fleet-scaling plan § 6.2) -------------

    /// Two vram cells (tight order puts the 1024 cell first), every third
    /// node "ineligible" so verification really filters.
    fn walk_fixture() -> (IndexSet, HashMap<String, Value>, HashMap<String, f64>) {
        let mut nodes = Vec::new();
        for n in 0..30 {
            nodes.push(make_node(
                &format!("a-{n:03}"),
                "default",
                json!({ "resources": { "vram": 1024 } }),
            ));
        }
        for n in 0..300 {
            nodes.push(make_node(
                &format!("b-{n:03}"),
                "default",
                json!({ "resources": { "vram": 65536 } }),
            ));
        }
        let (set, _) = set_for(
            json!({ "indexes": { "fit": false, "vram": ["pool", "resources.vram"] } }),
            nodes,
        );
        (
            set,
            HashMap::from([("pool".to_string(), json!("default"))]),
            HashMap::from([("vram".to_string(), 512.0)]),
        )
    }

    fn eligible(id: &str) -> Option<String> {
        let n: u32 = id[2..].parse().expect("numbered id");
        (n % 3 != 0).then(|| id.to_string())
    }

    fn walk(
        set: &IndexSet,
        b: &HashMap<String, Value>,
        r: &HashMap<String, f64>,
        budget: usize,
    ) -> IndexWalk<String> {
        set.walk_verified(b, r, budget, eligible).expect("walkable")
    }

    #[test]
    fn walk_equals_query_then_verify_when_untruncated() {
        let (set, bindings, request) = walk_fixture();
        let IndexQueryOutcome::Candidates(full) = set.query(&bindings, &request, None) else {
            panic!("expected candidates");
        };
        let mut expected: Vec<String> =
            full.node_ids.iter().filter_map(|id| eligible(id)).collect();
        let walked = walk(&set, &bindings, &request, 10_000);
        assert!(!walked.truncated);
        assert_eq!(walked.partition_size, 330);
        let mut got = walked.items;
        expected.sort();
        got.sort();
        assert_eq!(got, expected);
    }

    #[test]
    fn walk_keeps_whole_ranks_first_and_samples_the_boundary_uniformly() {
        let (set, bindings, request) = walk_fixture();
        // 20 eligible in the 1024 cell (kept whole), 200 in the boundary
        // cell, budget 40 -> 20 drawn from the boundary each time.
        let whole: Vec<String> = (0..30)
            .map(|n| format!("a-{n:03}"))
            .filter_map(|id| eligible(&id))
            .collect();
        let mut hits: HashMap<String, u32> = HashMap::new();
        let draws = 2_000;
        for _ in 0..draws {
            let walked = walk(&set, &bindings, &request, 40);
            assert!(walked.truncated);
            assert_eq!(walked.items.len(), 40);
            assert_eq!(
                &walked.items[..20],
                whole.as_slice(),
                "better rank whole, in order"
            );
            let boundary = &walked.items[20..];
            assert!(boundary.windows(2).all(|w| w[0] < w[1]), "cell order kept");
            for id in boundary {
                assert!(id.starts_with("b-") && eligible(id).is_some());
                *hits.entry(id.clone()).or_default() += 1;
            }
        }
        // 2,000 x 20 picks over 200 eligible members: 200 expected each.
        assert_eq!(
            hits.len(),
            200,
            "every eligible boundary member is reachable"
        );
        let (lo, hi) = (hits.values().min().unwrap(), hits.values().max().unwrap());
        assert!(*lo > 130 && *hi < 280, "not uniform: min {lo} max {hi}");
    }

    #[test]
    fn walk_reports_truncation_only_when_another_eligible_node_exists() {
        let (set, bindings, request) = walk_fixture();
        // 220 eligible in total. Exactly 220: nothing left, not truncated.
        let exact = walk(&set, &bindings, &request, 220);
        assert!(!exact.truncated);
        assert_eq!(exact.items.len(), 220);
        // 20 fills the first cell exactly; the next cell has more, so the
        // old path would have truncated and kept exactly the first cell.
        let filled = walk(&set, &bindings, &request, 20);
        assert!(filled.truncated);
        assert!(filled.items.iter().all(|id| id.starts_with("a-")));
        assert_eq!(filled.items.len(), 20);
    }

    #[test]
    fn walk_fills_the_budget_under_a_selective_filter() {
        let (set, bindings, request) = walk_fixture();
        // Only ids ending in 7 pass (~10%): still a full budget of 25.
        let walked = set
            .walk_verified(&bindings, &request, 25, |id| {
                id.ends_with('7').then(|| id.to_string())
            })
            .expect("walkable");
        assert_eq!(walked.items.len(), 25);
        assert!(walked.truncated);
        assert!(walked.items.iter().all(|id| id.ends_with('7')));
    }

    /// The sample only touches the cell the cut lands in: better-ranked
    /// cells are still included whole and in rank order.
    #[test]
    fn boundary_cell_truncation_keeps_whole_ranks_and_samples_the_cut() {
        // ranks: 0 (2 items), 1 (3 items), 2 (100 items); budget 10 → both
        // whole ranks (5) plus 5 sampled from rank 2, in order.
        let ranked: Vec<(usize, u32)> = (0..2)
            .map(|i| (i, 0))
            .chain((2..5).map(|i| (i, 1)))
            .chain((5..105).map(|i| (i, 2)))
            .collect();
        let mut union: HashSet<usize> = HashSet::new();
        for _ in 0..30 {
            let out = truncate_within_boundary_cell(ranked.clone(), 10);
            assert_eq!(out.len(), 10);
            assert_eq!(&out[..5], &[0, 1, 2, 3, 4]);
            assert!(out.windows(2).all(|w| w[0] < w[1]), "order kept: {out:?}");
            assert!(out[5..].iter().all(|i| (5..105).contains(i)));
            union.extend(out[5..].iter().copied());
        }
        assert!(
            union.len() > 40,
            "the cut is a prefix, not a sample: {}",
            union.len()
        );
        // No cut needed.
        assert_eq!(
            truncate_within_boundary_cell(vec![(7usize, 0), (8, 1)], 5),
            vec![7, 8]
        );
        // Budget 0.
        assert!(truncate_within_boundary_cell(ranked.clone(), 0).is_empty());
    }

    #[test]
    fn ordered_queries_carry_cell_ranks() {
        let (set, _) = set_for(
            json!({ "indexes": { "fit": false, "vram": ["pool", "resources.vram"] } }),
            fit_nodes(),
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = HashMap::from([("vram".to_string(), 512.0)]);
        let IndexQueryOutcome::Candidates(candidates) = set.query(&bindings, &request, None) else {
            panic!("expected candidates");
        };
        assert_eq!(
            candidates.node_ids,
            vec!["medium", "large", "small", "none"]
        );
        assert_eq!(candidates.cell_ranks, vec![0, 1, 2, 3]);
        let IndexQueryOutcome::Candidates(unordered) =
            set.query_with_order(&bindings, &request, None, false)
        else {
            panic!("expected candidates");
        };
        assert!(unordered.cell_ranks.is_empty());
    }

    #[test]
    fn budget_cut_keeps_better_cells_whole_and_first() {
        let mut nodes = vec![make_node(
            "exact",
            "default",
            json!({ "resources": { "vram": 600 } }),
        )];
        nodes.extend((0..100).map(|n| {
            make_node(
                &format!("loose-{n:03}"),
                "default",
                json!({ "resources": { "vram": 65536 } }),
            )
        }));
        let (set, _) = set_for(
            json!({ "indexes": { "fit": false, "vram": ["pool", "resources.vram"] } }),
            nodes,
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = HashMap::from([("vram".to_string(), 512.0)]);
        let IndexQueryOutcome::Candidates(candidates) = set.query(&bindings, &request, Some(8))
        else {
            panic!("expected candidates");
        };
        assert_eq!(candidates.node_ids.len(), 8);
        assert_eq!(
            candidates.node_ids[0], "exact",
            "tight order: nearest fit first"
        );
        assert!(candidates.node_ids[1..]
            .iter()
            .all(|id| id.starts_with("loose-")));
    }

    #[test]
    fn zero_request_matches_zero_free_nodes() {
        let (set, _) = set_for(
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
        let (mut set, mut nodes) = set_for(
            json!({ "indexes": { "fit": false, "topo": ["region"] } }),
            vec![make_node("a", "default", json!({ "region": "east" }))],
        );
        let east = HashMap::from([("region".to_string(), json!("east"))]);
        let west = HashMap::from([("region".to_string(), json!("west"))]);

        // Move a to west.
        nodes.get_mut("a").unwrap().config_attrs = json!({ "region": "west" });
        let node = nodes.get("a").unwrap();
        set.reindex_node(node, &pool_for(node));
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

        // Unavailable/deleted nodes leave via remove_node (the read model
        // decides membership), and removal is idempotent.
        set.remove_node("a");
        let IndexQueryOutcome::Candidates(candidates) = set.query(&west, &HashMap::new(), None)
        else {
            panic!()
        };
        assert!(candidates.node_ids.is_empty());
        set.remove_node("a");
    }

    #[test]
    fn min_nodes_zero_disables_queries() {
        let (set, _) = set_for(
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
        let (set, _) = set_for(
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
