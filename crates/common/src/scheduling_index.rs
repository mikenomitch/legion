//! Declarable scheduling indexes: ad-hoc, operator-defined candidate
//! generation for the scheduler (`internal_docs/plans/scheduling-index-plan.md`,
//! `internal_docs/plans/candidates-and-indexes-plan.md`, public doc
//! `docs/scheduling-indexes.md`).
//!
//! An index is `partition_by` (exact-match attribute keys, each distinct
//! value-tuple is a shard) times a geometry that projects every node of a
//! shard into a cell. The built-in geometry (`kind = "grid"`) is `dims`:
//! numeric dimensions bucketed into a sparse grid — free resource capacity via
//! `resources.*`, free device counts via `devices.*`, or any numeric node
//! attribute. A query picks the shard the task's equality constraints bind,
//! then walks the shard's occupied cells *nearest-fit first*: cells whose
//! every bucket covers the request's bucket ("the orthant"), ordered by
//! weighted bucket distance, then every remaining cell. Ordering is therefore
//! fit-informed but the untruncated candidate *set* is exactly the shard —
//! with an unbounded budget the scheduler's re-verified candidates equal
//! today's full scan, which is what the equivalence tests pin down.
//!
//! A plugin kind ([`IndexKind`]) replaces only the geometry: it projects a
//! node to a cell and ranks a cell for a request. Shards, maintenance, the
//! walk, truncation, and re-verification stay here, so a plugin can make the
//! order worse but never a placement invalid.
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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::sync::Arc;

use crate::constraints::{node_attr, value_token};
use crate::devices::{DeviceName, POOL_KEY_PREFIX};
use crate::prelude::*;
use crate::resources::{ResourcePool, ResourceRequest, ResourceValue};
use crate::structs::Node;

/// Default for `schedulerSettings.index_min_nodes`: below this many candidate
/// nodes no index arms and planning uses the plain scan. `0` disables
/// indexing entirely (the documented off-ramp).
pub const DEFAULT_INDEX_MIN_NODES: usize = 128;

/// Hard cap on dims per index — cells are tiny vectors and orthant walks
/// degrade with dimensionality, so this fails fast at parse time.
pub const MAX_DIMS_PER_INDEX: usize = 6;

/// The `indexHint` value that skips every index and scans, and therefore a
/// name no declared index may take.
pub const SCAN_HINT: &str = "scan";

/// The built-in geometry's kind name.
pub const GRID_KIND: &str = "grid";

/// The resource every task requests one of and `maxAllocations` caps. It is
/// never counted as selection coverage: every request names it, so counting
/// it would only tilt selection toward whichever index happens to carry it.
pub const ALLOCATIONS_RESOURCE: &str = "allocations";

/// Dim attrs with this prefix read free resource capacity; anything else
/// reads the node attribute chain (config -> fingerprint -> dynamic).
const RESOURCE_DIM_PREFIX: &str = "resources.";

/// Dim attrs with this prefix count free devices whose group matches the
/// device name after it (`devices.gpu`, `devices.nvidia/gpu/A100`).
const DEVICE_DIM_PREFIX: &str = "devices.";

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

/// Longest cell a plugin kind may project (longer ones are cut). Cells are
/// map keys compared on every walk, so they stay small.
pub const MAX_PLUGIN_CELL_LEN: usize = 16;

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

/// What a dim reads, decided once at parse time.
#[derive(Debug, Clone, PartialEq)]
pub enum DimSource {
    /// `resources.<name>`: free capacity. An amount reads as itself; an item
    /// set (a custom item resource, a device pool key) as its free count.
    Resource(String),
    /// `devices.<name>`: free devices across every group the name matches.
    Device(DeviceName),
    /// Any other key: a numeric node attribute.
    Attr(String),
}

impl DimSource {
    fn parse(index_name: &str, attr: &str) -> R<DimSource> {
        if let Some(name) = attr.strip_prefix(RESOURCE_DIM_PREFIX) {
            if name.is_empty() {
                bail!("index {index_name}: dim {attr} names no resource");
            }
            return Ok(DimSource::Resource(name.to_string()));
        }
        if let Some(name) = attr.strip_prefix(DEVICE_DIM_PREFIX) {
            let parsed = DeviceName::parse(name)
                .with_context(|| format!("index {index_name}: dim {attr}"))?;
            return Ok(DimSource::Device(parsed));
        }
        Ok(DimSource::Attr(attr.to_string()))
    }

    /// Counts toward selection coverage when the request asks for it.
    fn is_request_dim(&self) -> bool {
        match self {
            DimSource::Resource(name) => name != ALLOCATIONS_RESOURCE,
            DimSource::Device(_) => true,
            DimSource::Attr(_) => false,
        }
    }
}

/// One numeric dimension of an index.
#[derive(Debug, Clone)]
pub struct DimSpec {
    /// As declared: `resources.<name>`, `devices.<name>`, or an attribute.
    pub attr: String,
    /// Multiplies this dim's contribution to the ordering distance. `0`
    /// makes the dim a pure fit test (the built-in `allocations` dim).
    pub weight: f64,
    pub scale: DimScale,
    pub source: DimSource,
}

impl DimSpec {
    fn new(index_name: &str, attr: &str, weight: f64, scale: DimScale) -> R<DimSpec> {
        Ok(DimSpec {
            attr: attr.to_string(),
            weight,
            scale,
            source: DimSource::parse(index_name, attr)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Pluggable kinds
// ---------------------------------------------------------------------------

/// A cell's place in a walk, as an [`IndexKind`] judges it for one request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CellRank {
    /// False sends the cell to the walk's tail (after every fitting cell):
    /// the kind believes no node in it can take the request. The ledger
    /// re-checks either way.
    pub fits: bool,
    /// Lower walks first within the fitting (and within the tail) cells.
    pub distance: f64,
}

/// An index geometry (`kind = "<name>"` on an index declaration,
/// docs/scheduling-indexes.md "Index kinds"). Stateless: Legion owns the
/// shards and calls [`IndexKind::project`] under the read-model lock whenever
/// a node or its free capacity changes, and [`IndexKind::rank`] once per
/// occupied cell per query. Both must be cheap and must not block.
pub trait IndexKind: Send + Sync {
    /// The kind's name, for logs.
    fn name(&self) -> &'static str;

    /// One node's cell: a short vector (at most [`MAX_PLUGIN_CELL_LEN`]
    /// values; longer ones are cut). Nodes with equal cells are
    /// indistinguishable to the index.
    fn project(&self, node: &Node, free: &ResourcePool) -> Vec<u16>;

    /// Where a cell falls in the walk for `request`.
    fn rank(&self, cell: &[u16], request: &IndexRequest) -> CellRank;

    /// How many of `request`'s resources this kind orders by: automatic
    /// selection prefers the index that covers the most.
    fn coverage(&self, _request: &IndexRequest) -> usize {
        0
    }

    /// False once the kind can no longer be trusted (a guarded plugin that
    /// panicked). Selection then skips the index and planning scans.
    fn healthy(&self) -> bool {
        true
    }
}

impl<T: IndexKind + ?Sized> IndexKind for Box<T> {
    fn name(&self) -> &'static str {
        (**self).name()
    }
    fn project(&self, node: &Node, free: &ResourcePool) -> Vec<u16> {
        (**self).project(node, free)
    }
    fn rank(&self, cell: &[u16], request: &IndexRequest) -> CellRank {
        (**self).rank(cell, request)
    }
    fn coverage(&self, request: &IndexRequest) -> usize {
        (**self).coverage(request)
    }
    fn healthy(&self) -> bool {
        (**self).healthy()
    }
}

/// What a kind plugin exports: one [`IndexKind`] per index declared with
/// that kind, built from the declaration's free-form `options`.
pub trait IndexKindFactory: Send + Sync {
    /// The kind's name, for logs.
    fn name(&self) -> &'static str;

    /// The geometry for index `index_name`. An error fails manager startup,
    /// naming the index — the same posture as any malformed declaration.
    fn build(&self, index_name: &str, options: &Value) -> R<Box<dyn IndexKind>>;
}

/// A resolved plugin kind, shared by every clone of the settings.
#[derive(Clone)]
pub struct KindHandle(pub Arc<dyn IndexKind>);

impl std::fmt::Debug for KindHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "KindHandle({})", self.0.name())
    }
}

/// An index's geometry, as declared.
#[derive(Debug, Clone)]
pub enum IndexKindRef {
    /// The built-in grid over `dims`.
    Grid,
    /// `kind = "<name>"`: a plugin geometry, resolved after parsing
    /// ([`IndexSettings::resolve_kinds`]). `handle` stays `None` until then;
    /// an unresolved kind is never selected.
    Plugin {
        name: String,
        options: Value,
        handle: Option<KindHandle>,
    },
}

impl IndexKindRef {
    pub fn name(&self) -> &str {
        match self {
            IndexKindRef::Grid => GRID_KIND,
            IndexKindRef::Plugin { name, .. } => name,
        }
    }
}

/// A parsed index declaration.
#[derive(Debug, Clone)]
pub struct IndexSpec {
    pub name: String,
    pub partition_by: Vec<String>,
    pub dims: Vec<DimSpec>,
    pub order: IndexOrder,
    pub mode: IndexMode,
    pub kind: IndexKindRef,
}

impl IndexSpec {
    /// Why this index cannot serve any query right now, if it cannot:
    /// `off`, an unresolved plugin kind, or a quarantined one.
    fn unavailable(&self) -> Option<String> {
        if self.mode == IndexMode::Off {
            return Some(format!("{} is off", self.name));
        }
        match &self.kind {
            IndexKindRef::Grid => None,
            IndexKindRef::Plugin { name, handle, .. } => match handle {
                None => Some(format!("{}: kind {name} is not loaded", self.name)),
                Some(handle) if !handle.0.healthy() => Some(format!(
                    "{}: kind {name} is quarantined after a plugin failure",
                    self.name
                )),
                Some(_) => None,
            },
        }
    }

    fn coverage(&self, request: &IndexRequest) -> usize {
        match &self.kind {
            IndexKindRef::Grid => self
                .dims
                .iter()
                .filter(|dim| dim.source.is_request_dim() && request.dim_value(dim) > 0.0)
                .count(),
            IndexKindRef::Plugin { handle, .. } => handle
                .as_ref()
                .map_or(0, |handle| handle.0.coverage(request)),
        }
    }

    fn project(&self, node: &Node, pool: &ResourcePool) -> Vec<u16> {
        match &self.kind {
            IndexKindRef::Grid => self
                .dims
                .iter()
                .map(|dim| bucket_value(&dim.scale, dim_value(node, pool, dim)))
                .collect(),
            IndexKindRef::Plugin { handle, .. } => match handle {
                Some(handle) => {
                    let mut cell = handle.0.project(node, pool);
                    cell.truncate(MAX_PLUGIN_CELL_LEN);
                    cell
                }
                None => Vec::new(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// What a task (or a co-placed group) asks of an index: its resource amounts,
/// item-set sizes, device demands, and an optional `indexHint`.
#[derive(Debug, Clone, Default)]
pub struct IndexRequest {
    /// `Amount` resources by name.
    pub amounts: HashMap<String, f64>,
    /// `Items` resources by name: how many items the request names.
    pub item_counts: HashMap<String, f64>,
    /// Unassigned device demands: name and count.
    pub devices: Vec<(DeviceName, u32)>,
    /// The task's `indexHint`: an index name, or [`SCAN_HINT`].
    pub hint: Option<String>,
}

impl IndexRequest {
    /// A request of plain amounts (tests and simple callers).
    pub fn from_amounts(amounts: HashMap<String, f64>) -> IndexRequest {
        IndexRequest {
            amounts,
            ..IndexRequest::default()
        }
    }

    /// A task's normalized resource request, as the index sees it.
    pub fn from_resource_request(request: &ResourceRequest) -> IndexRequest {
        let mut out = IndexRequest::default();
        for (name, value) in &request.values {
            match value {
                ResourceValue::Amount(amount) => {
                    out.amounts.insert(name.clone(), *amount);
                }
                ResourceValue::Items(items) => {
                    out.item_counts.insert(name.clone(), items.len() as f64);
                }
            }
        }
        out.devices = request
            .devices
            .iter()
            .map(|demand| (demand.name.clone(), demand.count))
            .collect();
        out
    }

    pub fn with_hint(mut self, hint: Option<&str>) -> IndexRequest {
        self.hint = hint.map(str::to_string);
        self
    }

    /// Adds `other` to this request: what one node must hold for both (a
    /// multi-task group's members all land on one node).
    pub fn add(&mut self, other: &IndexRequest) {
        for (name, amount) in &other.amounts {
            *self.amounts.entry(name.clone()).or_default() += amount;
        }
        for (name, count) in &other.item_counts {
            *self.item_counts.entry(name.clone()).or_default() += count;
        }
        self.devices.extend(other.devices.iter().cloned());
        if self.hint.is_none() {
            self.hint = other.hint.clone();
        }
    }

    /// The request's value along `dim`: what a node must have free there.
    pub fn dim_value(&self, dim: &DimSpec) -> f64 {
        match &dim.source {
            DimSource::Resource(name) => self
                .amounts
                .get(name)
                .or_else(|| self.item_counts.get(name))
                .copied()
                .unwrap_or(0.0),
            DimSource::Device(wanted) => self
                .devices
                .iter()
                .filter(|(name, _)| name.compatible(wanted))
                .map(|(_, count)| f64::from(*count))
                .sum(),
            DimSource::Attr(_) => 0.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

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
///
/// The fourth dim, free `allocations` at weight 0, is a cap flag: it takes
/// part in the orthant test only. A node at its `maxAllocations` lands in the
/// non-fitting tail instead of a fitting cell (where, looking idle on cpu and
/// memory, it used to rank first under `bin_pack` and waste the candidate
/// budget), while every node with a free slot ranks exactly as before —
/// uncapped nodes (infinite free) and capped ones alike contribute nothing to
/// the distance.
fn default_fit_spec(order: IndexOrder) -> IndexSpec {
    let mut dims: Vec<DimSpec> = ["resources.cpu", "resources.memory", "resources.disk"]
        .iter()
        .map(|attr| DimSpec {
            attr: (*attr).to_string(),
            weight: 1.0,
            scale: DimScale::Log,
            source: DimSource::Resource(attr[RESOURCE_DIM_PREFIX.len()..].to_string()),
        })
        .collect();
    dims.push(DimSpec {
        attr: format!("{RESOURCE_DIM_PREFIX}{ALLOCATIONS_RESOURCE}"),
        weight: 0.0,
        scale: DimScale::Log,
        source: DimSource::Resource(ALLOCATIONS_RESOURCE.to_string()),
    });
    IndexSpec {
        name: "fit".to_string(),
        partition_by: vec!["pool".to_string()],
        dims,
        order,
        mode: IndexMode::On,
        kind: IndexKindRef::Grid,
    }
}

/// Which index serves a query, and anything the explain surface should say
/// about how it was chosen (an ignored hint).
#[derive(Debug, Clone)]
pub struct Selection {
    pub index: usize,
    pub note: Option<String>,
}

impl IndexSettings {
    /// Parses `schedulerSettings` into index settings. `scheduling_policy`
    /// supplies the default ordering: `spread` prefers headroom (`loose`),
    /// everything else packs (`tight`). Unknown fields and malformed specs
    /// fail loudly — indexes silently doing nothing is the failure mode the
    /// explain surface exists to prevent, and that starts at parse time.
    ///
    /// Plugin kinds parse into unresolved [`IndexKindRef::Plugin`] entries;
    /// the manager resolves them with [`IndexSettings::resolve_kinds`].
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
                if name == SCAN_HINT {
                    bail!("index name {SCAN_HINT} is reserved: `indexHint = \"{SCAN_HINT}\"` means no index");
                }
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

    /// The plugin kind names the declarations use, deduplicated.
    pub fn plugin_kinds(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .indexes
            .iter()
            .filter_map(|spec| match &spec.kind {
                IndexKindRef::Plugin { name, .. } => Some(name.clone()),
                IndexKindRef::Grid => None,
            })
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// Resolves every plugin-kind declaration: `build(kind, index, options)`
    /// returns the geometry for one index. An error names the index and
    /// fails the whole parse, like any malformed declaration.
    pub fn resolve_kinds(
        &mut self,
        mut build: impl FnMut(&str, &str, &Value) -> R<Arc<dyn IndexKind>>,
    ) -> R<()> {
        for spec in &mut self.indexes {
            if let IndexKindRef::Plugin {
                name,
                options,
                handle,
            } = &mut spec.kind
            {
                let kind = build(name, &spec.name, options)
                    .with_context(|| format!("index {}: kind {name}", spec.name))?;
                *handle = Some(KindHandle(kind));
            }
        }
        Ok(())
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

    /// Picks the index for a task: usable means available (not `off`, its
    /// kind loaded and healthy) and every `partition_by` key bound by
    /// `bindings`; best coverage of the task's requested resources wins,
    /// then more partition keys, then declaration order. `Err` carries the
    /// human-readable reason for the explain surface.
    pub fn select(
        &self,
        bindings: &HashMap<String, Value>,
        request: &IndexRequest,
    ) -> Result<usize, String> {
        self.select_hinted(bindings, request)
            .map(|selection| selection.index)
    }

    /// [`Self::select`], honoring `request.hint`: a hinted index that is
    /// usable wins outright; one that is not falls back to automatic
    /// selection, with a note saying why. A [`SCAN_HINT`] is the caller's
    /// to honor before querying; here it selects nothing.
    pub fn select_hinted(
        &self,
        bindings: &HashMap<String, Value>,
        request: &IndexRequest,
    ) -> Result<Selection, String> {
        self.select_where(|key| bindings.contains_key(key), request, None)
    }

    /// Selection core: `bound(key)` says whether a partition key is bound,
    /// and `require` (partition enumeration) admits only indexes whose
    /// `partition_by` contains that key.
    fn select_where(
        &self,
        bound: impl Fn(&str) -> bool,
        request: &IndexRequest,
        require: Option<&str>,
    ) -> Result<Selection, String> {
        if self.indexes.is_empty() {
            return Err("no indexes configured".to_string());
        }
        if request.hint.as_deref() == Some(SCAN_HINT) {
            return Err(format!("indexHint = \"{SCAN_HINT}\""));
        }
        let usable = |spec: &IndexSpec| -> Result<(), String> {
            if let Some(reason) = spec.unavailable() {
                return Err(reason);
            }
            if let Some(key) = require {
                if !spec.partition_by.iter().any(|partition| partition == key) {
                    return Err(format!("{} does not partition by {key}", spec.name));
                }
            }
            let missing: Vec<&str> = spec
                .partition_by
                .iter()
                .filter(|key| !bound(key.as_str()))
                .map(String::as_str)
                .collect();
            if missing.is_empty() {
                Ok(())
            } else {
                Err(format!("{} needs [{}]", spec.name, missing.join(", ")))
            }
        };

        let mut note = None;
        if let Some(hint) = request.hint.as_deref() {
            match self.indexes.iter().position(|spec| spec.name == hint) {
                Some(idx) => match usable(&self.indexes[idx]) {
                    Ok(()) => {
                        return Ok(Selection {
                            index: idx,
                            note: None,
                        })
                    }
                    Err(reason) => note = Some(format!("hint {hint} unusable: {reason}")),
                },
                None => note = Some(format!("hint {hint} names no declared index")),
            }
        }

        let mut best: Option<(usize, usize, usize)> = None;
        let mut unusable = Vec::new();
        let mut every_off = true;
        for (idx, spec) in self.indexes.iter().enumerate() {
            if spec.mode != IndexMode::Off {
                every_off = false;
            }
            if spec.mode == IndexMode::Off {
                continue;
            }
            if let Err(reason) = usable(spec) {
                unusable.push(reason);
                continue;
            }
            let candidate = (spec.coverage(request), spec.partition_by.len(), idx);
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
        let with_note = |reason: String| match &note {
            Some(note) => format!("{note}; {reason}"),
            None => reason,
        };
        match best {
            Some((_, _, idx)) => Ok(Selection { index: idx, note }),
            None if every_off => Err(with_note("every index is off".to_string())),
            None => Err(with_note(format!(
                "no usable index: {}",
                unusable.join("; ")
            ))),
        }
    }
}

fn parse_index_spec(name: &str, value: &Value, default_order: IndexOrder) -> R<IndexSpec> {
    let (partition_by, dims, order, mode, kind) = match value {
        // Shorthand: a list of attribute keys. `resources.*` and `devices.*`
        // entries become dims, everything else partitions.
        Value::Array(items) => {
            let mut partition_by = Vec::new();
            let mut dims = Vec::new();
            for item in items {
                let key = item
                    .as_str()
                    .with_context(|| format!("index {name}: shorthand entries must be strings"))?;
                if key.starts_with(RESOURCE_DIM_PREFIX) || key.starts_with(DEVICE_DIM_PREFIX) {
                    dims.push(DimSpec::new(name, key, 1.0, DimScale::Log)?);
                } else {
                    partition_by.push(key.to_string());
                }
            }
            (
                partition_by,
                dims,
                default_order,
                IndexMode::On,
                IndexKindRef::Grid,
            )
        }
        Value::Object(map) => {
            for key in map.keys() {
                if !matches!(
                    key.as_str(),
                    "partition_by" | "dims" | "order" | "mode" | "kind" | "options"
                ) {
                    bail!("index {name}: unknown field {key} (expected partition_by, dims, order, mode, kind, options)");
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
            let options = map.get("options").cloned().unwrap_or(Value::Null);
            let kind = match map.get("kind") {
                None | Some(Value::Null) => IndexKindRef::Grid,
                Some(Value::String(kind)) if kind == GRID_KIND => IndexKindRef::Grid,
                Some(Value::String(kind)) => {
                    if kind.is_empty()
                        || !kind
                            .chars()
                            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
                    {
                        bail!("index {name}: kind must be a plugin name (letters, digits, `_`, `-`), got {kind:?}");
                    }
                    if !dims.is_empty() {
                        bail!("index {name}: dims belong to the grid kind; a {kind} index takes its geometry from the plugin (configure it with options)");
                    }
                    if map.contains_key("order") {
                        bail!("index {name}: order belongs to the grid kind; a {kind} index ranks cells itself");
                    }
                    IndexKindRef::Plugin {
                        name: kind.clone(),
                        options: options.clone(),
                        handle: None,
                    }
                }
                Some(_) => bail!("index {name}: kind must be a string"),
            };
            if matches!(kind, IndexKindRef::Grid) && !options.is_null() {
                bail!("index {name}: options only apply to a plugin kind");
            }
            (partition_by, dims, order, mode, kind)
        }
        _ => bail!("index {name}: must be false, a list of attribute keys, or an object"),
    };

    if partition_by.is_empty() && dims.is_empty() && matches!(kind, IndexKindRef::Grid) {
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
        kind,
    })
}

fn parse_dim_spec(index_name: &str, value: &Value) -> R<DimSpec> {
    match value {
        Value::String(attr) => DimSpec::new(index_name, attr, 1.0, DimScale::Log),
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
            DimSpec::new(index_name, &attr, weight, scale)
        }
        _ => bail!("index {index_name}: dims must be strings or objects"),
    }
}

// ---------------------------------------------------------------------------
// Live state
// ---------------------------------------------------------------------------

/// One node's projection into one index: shard key and cell.
type NodeProjection = (String, Vec<u16>);

#[derive(Debug, Clone, Default)]
struct Shard {
    /// The partition tokens this shard's key joins, in `partition_by`
    /// order: what partition enumeration matches against.
    tokens: Vec<String>,
    /// Cell -> node ids in it, ids kept sorted for deterministic output.
    cells: BTreeMap<Vec<u16>, Vec<String>>,
}

#[derive(Debug, Clone, Default)]
struct IndexState {
    shards: HashMap<String, Shard>,
    node_records: HashMap<String, NodeProjection>,
}

impl IndexState {
    fn insert(
        &mut self,
        node_id: &str,
        projection: NodeProjection,
        tokens: impl FnOnce() -> Vec<String>,
    ) {
        let shard = self
            .shards
            .entry(projection.0.clone())
            .or_insert_with(|| Shard {
                tokens: tokens(),
                cells: BTreeMap::new(),
            });
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
    /// How selection went, when there is something to say (an ignored
    /// `indexHint`).
    pub note: Option<String>,
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
    /// See [`IndexCandidates::note`].
    pub note: Option<String>,
}

/// The shards a `count.per` key splits a query into
/// ([`IndexSet::enumerate_partitions`]): one per value of the key.
#[derive(Debug, Clone)]
pub struct PartitionEnumeration {
    pub index: String,
    /// Position of the serving index in the settings.
    pub index_pos: usize,
    /// `(per-key value token, shard key)`, sorted by token. The token is
    /// `value_token` of the node attribute, the same token
    /// `count.per` grouping uses.
    pub groups: Vec<(String, String)>,
    pub note: Option<String>,
}

/// One ranked cell in a walk. Ordered so a [`BinaryHeap`] (a max-heap) pops
/// the best cell first: fitting before the tail, lower distance first, then
/// the cell key for a deterministic walk.
struct HeapCell<'s> {
    tail: bool,
    distance: f64,
    key: &'s Vec<u16>,
    ids: &'s Vec<String>,
}

impl HeapCell<'_> {
    fn order(&self, other: &Self) -> Ordering {
        self.tail
            .cmp(&other.tail)
            .then(self.distance.total_cmp(&other.distance))
            .then(self.key.cmp(other.key))
    }
}

impl PartialEq for HeapCell<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.order(other) == Ordering::Equal
    }
}
impl Eq for HeapCell<'_> {}
impl PartialOrd for HeapCell<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapCell<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed: the max-heap's top is the smallest (best) cell.
        other.order(self)
    }
}

/// A shard's cells in walk order, produced lazily: ranking is O(cells) up
/// front, and each pop is O(log cells). A walk that stops after a few cells
/// (the budgeted walk) never pays for ordering the rest — the incremental
/// enumeration with early termination the grid deferred in v1
/// (scheduling-index calls 8).
enum CellWalk<'s> {
    Unordered(std::collections::btree_map::Values<'s, Vec<u16>, Vec<String>>),
    Ordered(BinaryHeap<HeapCell<'s>>),
}

impl<'s> Iterator for CellWalk<'s> {
    type Item = &'s Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            CellWalk::Unordered(values) => values.next(),
            CellWalk::Ordered(heap) => heap.pop().map(|cell| cell.ids),
        }
    }
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
    pub fn reindex_node(&mut self, node: &Node, pool: &ResourcePool) {
        if self.settings.indexes.is_empty() {
            return;
        }
        for (spec, state) in self.settings.indexes.iter().zip(self.states.iter_mut()) {
            let projection = project_node(spec, node, pool);
            if state.node_records.get(node.id.as_str()) == Some(&projection) {
                continue;
            }
            state.remove(&node.id);
            state.insert(&node.id, projection, || partition_tokens(spec, node));
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
        request: &IndexRequest,
        budget: Option<usize>,
    ) -> IndexQueryOutcome {
        self.query_with_order(bindings, request, budget, true)
    }

    /// The index this query uses and its shard's occupied cells, walked
    /// best-first when `ordered` (the cell rank is the position here).
    /// `Err` carries an outcome that needs no walk: indexing disabled, no
    /// usable index, or an empty partition (an exact, empty answer).
    fn select_cells<'s>(
        &'s self,
        bindings: &HashMap<String, Value>,
        request: &IndexRequest,
        ordered: bool,
    ) -> Result<(&'s IndexSpec, CellWalk<'s>, usize, Option<String>), IndexQueryOutcome> {
        if self.settings.indexes.is_empty() || self.settings.min_nodes == 0 {
            return Err(IndexQueryOutcome::Disabled);
        }
        let Selection { index: idx, note } = match self.settings.select_hinted(bindings, request) {
            Ok(selection) => selection,
            Err(reason) => return Err(IndexQueryOutcome::Unusable { reason }),
        };
        let spec = &self.settings.indexes[idx];
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

        match self.shard_walk(idx, &partition, request, ordered) {
            Some((walk, size)) => Ok((spec, walk, size, note)),
            // No node carries this exact attribute tuple. The scan would
            // reject every node on the same equality constraint, so an empty
            // candidate set is the *correct* answer, not a fallback case.
            None => Err(IndexQueryOutcome::Candidates(IndexCandidates {
                index: spec.name.clone(),
                mode: spec.mode,
                node_ids: Vec::new(),
                cell_ranks: Vec::new(),
                partition_size: 0,
                truncated: false,
                note,
            })),
        }
    }

    /// One shard's cells as a walk, with the shard's population. `None` for
    /// a shard no node occupies.
    fn shard_walk<'s>(
        &'s self,
        idx: usize,
        partition: &str,
        request: &IndexRequest,
        ordered: bool,
    ) -> Option<(CellWalk<'s>, usize)> {
        let spec = &self.settings.indexes[idx];
        let shard = self.states[idx].shards.get(partition)?;
        let size = shard.cells.values().map(Vec::len).sum();
        if !ordered {
            return Some((CellWalk::Unordered(shard.cells.values()), size));
        }
        // Rank every occupied cell once — the orthant (every bucket covers
        // the request's) first by weighted distance, ascending for tight and
        // descending for loose, then the provably-non-fitting tail, closest
        // first; a plugin kind ranks cells itself — and heapify. The walk
        // pops cells in order and stops when it has what it needs.
        let heap: BinaryHeap<HeapCell<'_>> = match &spec.kind {
            IndexKindRef::Grid => {
                let request_cell: Vec<u16> = spec
                    .dims
                    .iter()
                    .map(|dim| bucket_value(&dim.scale, request.dim_value(dim)))
                    .collect();
                shard
                    .cells
                    .iter()
                    .map(|(key, ids)| {
                        let fits = key
                            .iter()
                            .zip(request_cell.iter())
                            .all(|(have, want)| have >= want);
                        let distance: f64 = key
                            .iter()
                            .zip(request_cell.iter())
                            .zip(spec.dims.iter())
                            .map(|((have, want), dim)| {
                                dim.weight * (f64::from(*have) - f64::from(*want)).abs()
                            })
                            .sum();
                        let distance = match (fits, spec.order) {
                            (true, IndexOrder::Loose) => -distance,
                            _ => distance,
                        };
                        HeapCell {
                            tail: !fits,
                            distance,
                            key,
                            ids,
                        }
                    })
                    .collect()
            }
            IndexKindRef::Plugin { handle, .. } => {
                let handle = handle.as_ref()?;
                shard
                    .cells
                    .iter()
                    .map(|(key, ids)| {
                        let rank = handle.0.rank(key, request);
                        HeapCell {
                            tail: !rank.fits,
                            distance: if rank.distance.is_nan() {
                                f64::INFINITY
                            } else {
                                rank.distance
                            },
                            key,
                            ids,
                        }
                    })
                    .collect()
            }
        };
        Some((CellWalk::Ordered(heap), size))
    }

    pub fn query_with_order(
        &self,
        bindings: &HashMap<String, Value>,
        request: &IndexRequest,
        budget: Option<usize>,
        ordered: bool,
    ) -> IndexQueryOutcome {
        let (spec, cells, partition_size, note) =
            match self.select_cells(bindings, request, ordered) {
                Ok(selected) => selected,
                Err(outcome) => return outcome,
            };
        // Tag every id with its cell's rank in the walk. Within a cell the
        // index has no opinion, so a budget cut that lands inside a cell
        // samples it (`truncate_within_boundary_cell`) — the sorted-id
        // prefix it used to take is what packed a 5,000-node fleet onto
        // its lexicographically-first 1,250 nodes (aws-phase5-findings.md).
        // The scheduler asks for the whole shard and truncates AFTER
        // re-verification, so the ranks travel with the ids.
        let mut ranked: Vec<(String, u32)> = Vec::with_capacity(partition_size);
        for (rank, ids) in cells.enumerate() {
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
            note,
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
    /// Cells are walked best-first, popped from a heap, so the walk orders
    /// only the cells it reaches. A cell that fits in the remaining budget
    /// is verified whole and kept in order. The first cell that does not
    /// fit is drawn in uniformly random order and verified one member at a
    /// time: the first `remaining` verified members are a uniform sample of
    /// the cell's verified members, which is exactly what
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
        request: &IndexRequest,
        budget: usize,
        verify: impl FnMut(&str) -> Option<T>,
    ) -> Result<IndexWalk<T>, IndexQueryOutcome> {
        let (spec, cells, partition_size, note) = self.select_cells(bindings, request, true)?;
        if spec.mode != IndexMode::On {
            return Err(self.query_with_order(bindings, request, None, true));
        }
        let (items, truncated) = walk_cells(cells, partition_size, budget, verify);
        Ok(IndexWalk {
            index: spec.name.clone(),
            items,
            partition_size,
            truncated,
            note,
        })
    }

    /// Partition enumeration for `count.per = per_key`: picks an `on` index
    /// whose `partition_by` contains `per_key` and whose other keys are all
    /// bound, and lists its shards that match the bindings — one per value
    /// of `per_key` that some node carries. `None` when no index can serve
    /// it (the caller scans and groups). `per_key` values of `pool` and
    /// `id` never enumerate: `count.per` groups those by different rules
    /// than partitions do.
    pub fn enumerate_partitions(
        &self,
        bindings: &HashMap<String, Value>,
        request: &IndexRequest,
        per_key: &str,
    ) -> Option<PartitionEnumeration> {
        if self.settings.indexes.is_empty()
            || self.settings.min_nodes == 0
            || matches!(per_key, "pool" | "id")
        {
            return None;
        }
        let Selection { index: idx, note } = self
            .settings
            .select_where(
                |key| key == per_key || bindings.contains_key(key),
                request,
                Some(per_key),
            )
            .ok()?;
        let spec = &self.settings.indexes[idx];
        if spec.mode != IndexMode::On {
            return None;
        }
        let per_position = spec.partition_by.iter().position(|key| key == per_key)?;
        let wanted: Vec<Option<String>> = spec
            .partition_by
            .iter()
            .map(|key| {
                if key == per_key {
                    None
                } else {
                    bindings.get(key).map(value_token)
                }
            })
            .collect();
        let mut groups: Vec<(String, String)> = self.states[idx]
            .shards
            .iter()
            .filter(|(_, shard)| {
                shard.tokens.len() == wanted.len()
                    && shard.tokens[per_position] != ABSENT_TOKEN
                    && shard
                        .tokens
                        .iter()
                        .zip(wanted.iter())
                        .all(|(have, want)| want.as_ref().is_none_or(|want| want == have))
            })
            .map(|(key, shard)| (shard.tokens[per_position].clone(), key.clone()))
            .collect();
        groups.sort();
        Some(PartitionEnumeration {
            index: spec.name.clone(),
            index_pos: idx,
            groups,
            note,
        })
    }

    /// [`Self::walk_verified`] over one shard of an enumeration
    /// ([`Self::enumerate_partitions`]). An empty or vanished shard walks
    /// to nothing.
    pub fn walk_shard_verified<T>(
        &self,
        index_pos: usize,
        shard: &str,
        request: &IndexRequest,
        budget: usize,
        verify: impl FnMut(&str) -> Option<T>,
    ) -> IndexWalk<T> {
        let name = self
            .settings
            .indexes
            .get(index_pos)
            .map(|spec| spec.name.clone())
            .unwrap_or_default();
        match self.shard_walk(index_pos, shard, request, true) {
            Some((cells, partition_size)) => {
                let (items, truncated) = walk_cells(cells, partition_size, budget, verify);
                IndexWalk {
                    index: name,
                    items,
                    partition_size,
                    truncated,
                    note: None,
                }
            }
            None => IndexWalk {
                index: name,
                items: Vec::new(),
                partition_size: 0,
                truncated: false,
                note: None,
            },
        }
    }
}

/// The budgeted walk shared by [`IndexSet::walk_verified`] and
/// [`IndexSet::walk_shard_verified`]: `(kept, truncated)`.
fn walk_cells<'s, T>(
    cells: impl Iterator<Item = &'s Vec<String>>,
    partition_size: usize,
    budget: usize,
    mut verify: impl FnMut(&str) -> Option<T>,
) -> (Vec<T>, bool) {
    use rand::Rng;

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
    (out, truncated)
}

pub fn project_node(spec: &IndexSpec, node: &Node, pool: &ResourcePool) -> NodeProjection {
    let partition = spec
        .partition_by
        .iter()
        .map(|key| partition_token(node, key))
        .collect::<Vec<_>>()
        .join(&PARTITION_SEPARATOR.to_string());
    (partition, spec.project(node, pool))
}

fn partition_tokens(spec: &IndexSpec, node: &Node) -> Vec<String> {
    spec.partition_by
        .iter()
        .map(|key| partition_token(node, key))
        .collect()
}

/// `pool` reads the node's pool column (the same special case permits use);
/// everything else reads the attribute chain.
pub fn partition_token(node: &Node, key: &str) -> String {
    if key == "pool" {
        return value_token(&Value::String(node.pool.clone()));
    }
    node_attr(node, key)
        .map(value_token)
        .unwrap_or_else(|| ABSENT_TOKEN.to_string())
}

/// A missing or non-numeric dim value projects to zero — the node matches
/// requests that don't ask for the dim and sorts last for ones that do. An
/// item-set resource reads as its free item count; a device dim as the
/// free devices of every group its name matches.
pub fn dim_value(node: &Node, pool: &ResourcePool, dim: &DimSpec) -> f64 {
    match &dim.source {
        DimSource::Resource(resource) => match pool.values.get(resource) {
            Some(ResourceValue::Amount(amount)) => *amount,
            Some(ResourceValue::Items(items)) => items.len() as f64,
            None => 0.0,
        },
        DimSource::Device(wanted) => pool
            .values
            .iter()
            .filter_map(|(key, value)| {
                let group = key.strip_prefix(POOL_KEY_PREFIX)?;
                let ResourceValue::Items(free) = value else {
                    return None;
                };
                let name = DeviceName::parse(group).ok()?;
                name.compatible(wanted).then_some(free.len() as f64)
            })
            .sum(),
        DimSource::Attr(attr) => node_attr(node, attr).and_then(Value::as_f64).unwrap_or(0.0),
    }
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
        assert_eq!(fit.dims.len(), 4);
        assert_eq!(fit.dims[3].attr, "resources.allocations");
        assert_eq!(fit.dims[3].weight, 0.0);
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
        let outcome = set.query(&bindings, &IndexRequest::default(), None);
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
        let IndexQueryOutcome::Candidates(candidates) =
            set.query(&bindings, &IndexRequest::default(), None)
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
        let outcome = set.query(&bindings, &IndexRequest::default(), None);
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
        let request = IndexRequest::from_amounts(HashMap::from([("gpu_vram".to_string(), 1024.0)]));
        let idx = parsed.select(&bindings, &request).expect("selects");
        assert_eq!(parsed.indexes[idx].name, "gpu");
        // No gpu request: equal coverage (0), more partition keys wins.
        let idx = parsed
            .select(&bindings, &IndexRequest::default())
            .expect("selects");
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
        assert!(parsed.select(&bindings, &IndexRequest::default()).is_err());
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
        let request = IndexRequest::from_amounts(HashMap::from([("vram".to_string(), 512.0)]));
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
        let request = IndexRequest::from_amounts(HashMap::from([("vram".to_string(), 512.0)]));
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
        let request = IndexRequest::from_amounts(HashMap::from([("vram".to_string(), 512.0)]));
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
        let request = IndexRequest::from_amounts(HashMap::from([("vram".to_string(), 512.0)]));
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
        let request = IndexRequest::from_amounts(HashMap::from([("vram".to_string(), 512.0)]));

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
    fn walk_fixture() -> (IndexSet, HashMap<String, Value>, IndexRequest) {
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
            IndexRequest::from_amounts(HashMap::from([("vram".to_string(), 512.0)])),
        )
    }

    fn eligible(id: &str) -> Option<String> {
        let n: u32 = id[2..].parse().expect("numbered id");
        (!n.is_multiple_of(3)).then(|| id.to_string())
    }

    fn walk(
        set: &IndexSet,
        b: &HashMap<String, Value>,
        r: &IndexRequest,
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
        let request = IndexRequest::from_amounts(HashMap::from([("vram".to_string(), 512.0)]));
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
        let request = IndexRequest::from_amounts(HashMap::from([("vram".to_string(), 512.0)]));
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
        let IndexQueryOutcome::Candidates(candidates) =
            set.query(&bindings, &IndexRequest::default(), None)
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
        let IndexQueryOutcome::Candidates(candidates) =
            set.query(&east, &IndexRequest::default(), None)
        else {
            panic!()
        };
        assert!(candidates.node_ids.is_empty());
        let IndexQueryOutcome::Candidates(candidates) =
            set.query(&west, &IndexRequest::default(), None)
        else {
            panic!()
        };
        assert_eq!(candidates.node_ids, vec!["a"]);

        // Unavailable/deleted nodes leave via remove_node (the read model
        // decides membership), and removal is idempotent.
        set.remove_node("a");
        let IndexQueryOutcome::Candidates(candidates) =
            set.query(&west, &IndexRequest::default(), None)
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
            set.query(&bindings, &IndexRequest::default(), None),
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
        let IndexQueryOutcome::Candidates(candidates) =
            set.query(&bindings, &IndexRequest::default(), None)
        else {
            panic!()
        };
        assert_eq!(candidates.node_ids, vec!["a"]);
    }

    // --- candidates-and-indexes plan ---------------------------------------

    fn ids_of(outcome: IndexQueryOutcome) -> Vec<String> {
        match outcome {
            IndexQueryOutcome::Candidates(candidates) => candidates.node_ids,
            other => panic!("expected candidates, got {other:?}"),
        }
    }

    /// § 6.1: a node at its `maxAllocations` falls to the non-fitting tail
    /// of the default `fit` index; a capped node with room and an uncapped
    /// node keep their cpu/memory order (the allocations dim weighs 0).
    #[test]
    fn fit_sends_nodes_at_their_allocation_cap_to_the_tail() {
        let (set, _) = set_for(
            json!({ "index_min_nodes": 1 }),
            vec![
                make_node("full", "default", json!({ "maxAllocations": 0 })),
                make_node("room", "default", json!({ "maxAllocations": 4 })),
                make_node("open", "default", json!({})),
            ],
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = IndexRequest::from_amounts(HashMap::from([
            (ALLOCATIONS_RESOURCE.to_string(), 1.0),
            ("memory".to_string(), 64.0),
        ]));
        let ids = ids_of(set.query(&bindings, &request, None));
        assert_eq!(ids.last().map(String::as_str), Some("full"), "{ids:?}");
        assert_eq!(ids.len(), 3);
        // A budget of two keeps both nodes with a free slot.
        let mut kept = ids_of(set.query(&bindings, &request, Some(2)));
        kept.sort();
        assert_eq!(kept, vec!["open", "room"]);
    }

    #[test]
    fn coverage_ignores_the_allocations_resource() {
        let parsed = settings(json!({
            "indexes": { "gpu": ["pool", "region", "resources.gpu_vram", "resources.memory"] }
        }));
        let bindings = HashMap::from([
            ("pool".to_string(), json!("default")),
            ("region".to_string(), json!("east")),
        ]);
        // fit covers cpu + memory (+ allocations, which must not count);
        // gpu covers gpu_vram + memory and has more partition keys.
        let request = IndexRequest::from_amounts(HashMap::from([
            ("cpu".to_string(), 100.0),
            ("memory".to_string(), 64.0),
            ("gpu_vram".to_string(), 1024.0),
            (ALLOCATIONS_RESOURCE.to_string(), 1.0),
        ]));
        let idx = parsed.select(&bindings, &request).expect("selects");
        assert_eq!(parsed.indexes[idx].name, "gpu");
    }

    /// § 6.10: the heap walk yields exactly the order a full sort would.
    #[test]
    fn heap_walk_matches_a_full_sort_of_the_cells() {
        let mut nodes = Vec::new();
        for n in 0..64u32 {
            nodes.push(make_node(
                &format!("n-{n:03}"),
                "default",
                json!({ "resources": { "vram": 1u64 << (n % 13), "disk": 100 * (n % 5) } }),
            ));
        }
        let (set, _) = set_for(
            json!({ "indexes": { "fit": false, "x": ["pool", "resources.vram", "resources.disk"] } }),
            nodes,
        );
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = IndexRequest::from_amounts(HashMap::from([
            ("vram".to_string(), 256.0),
            ("disk".to_string(), 150.0),
        ]));
        let IndexQueryOutcome::Candidates(candidates) = set.query(&bindings, &request, None) else {
            panic!()
        };
        // Recompute the order by brute force from each node's projection.
        let spec = &set.settings().indexes[0];
        let want: Vec<u16> = spec
            .dims
            .iter()
            .map(|dim| bucket_value(&dim.scale, request.dim_value(dim)))
            .collect();
        let key = |id: &str| {
            let cell = &set.states[0].node_records[id].1;
            let fits = cell.iter().zip(&want).all(|(have, want)| have >= want);
            let distance: f64 = cell
                .iter()
                .zip(&want)
                .map(|(have, want)| (f64::from(*have) - f64::from(*want)).abs())
                .sum();
            (!fits, distance, cell.clone(), id.to_string())
        };
        let mut expected = candidates.node_ids.clone();
        expected.sort_by(|a, b| {
            let (a, b) = (key(a), key(b));
            a.0.cmp(&b.0)
                .then(a.1.total_cmp(&b.1))
                .then(a.2.cmp(&b.2))
                .then(a.3.cmp(&b.3))
        });
        assert_eq!(candidates.node_ids, expected);
    }

    /// § 6.8: an item-set resource and a device dim bucket by free count,
    /// and the request side counts items and matching device demands.
    #[test]
    fn device_and_item_dims_bucket_by_free_count() {
        let dims = settings(json!({
            "indexes": { "fit": false, "gpu": ["pool", "devices.nvidia/gpu", "resources.slots"] }
        }));
        let spec = &dims.indexes[0];
        assert!(matches!(spec.dims[0].source, DimSource::Device(_)));
        let node = make_node("a", "default", json!({}));
        let items =
            |ids: &[&str]| ResourceValue::Items(ids.iter().map(|id| id.to_string()).collect());
        let pool = ResourcePool {
            values: HashMap::from([
                (
                    "device:nvidia/gpu/A100".to_string(),
                    items(&["0", "1", "2"]),
                ),
                ("device:nvidia/gpu/H100".to_string(), items(&["3"])),
                ("device:amd/gpu/MI300".to_string(), items(&["4", "5"])),
                ("slots".to_string(), items(&["x", "y"])),
            ]),
            ..ResourcePool::default()
        };
        assert_eq!(dim_value(&node, &pool, &spec.dims[0]), 4.0);
        assert_eq!(dim_value(&node, &pool, &spec.dims[1]), 2.0);

        let request = IndexRequest {
            devices: vec![
                (DeviceName::parse("gpu").expect("name"), 2),
                (DeviceName::parse("amd/gpu").expect("name"), 5),
            ],
            item_counts: HashMap::from([("slots".to_string(), 1.0)]),
            ..IndexRequest::default()
        };
        // `gpu` could be an nvidia gpu, `amd/gpu` cannot.
        assert_eq!(request.dim_value(&spec.dims[0]), 2.0);
        assert_eq!(request.dim_value(&spec.dims[1]), 1.0);
        assert_eq!(spec.coverage(&request), 2);
    }

    #[test]
    fn device_dims_order_nodes_by_free_devices() {
        let parsed = Arc::new(settings(json!({
            "index_min_nodes": 1,
            "indexes": { "fit": false, "gpu": ["pool", "devices.gpu"] },
        })));
        let mut set = IndexSet::new(parsed);
        for (id, free) in [("one", 1usize), ("four", 4), ("eight", 8)] {
            let pool = ResourcePool {
                values: HashMap::from([(
                    format!("device:nvidia/gpu/{id}"),
                    ResourceValue::Items((0..free).map(|n| n.to_string()).collect()),
                )]),
                ..ResourcePool::default()
            };
            set.reindex_node(&make_node(id, "default", json!({})), &pool);
        }
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request = IndexRequest {
            devices: vec![(DeviceName::parse("nvidia/gpu").expect("name"), 4)],
            ..IndexRequest::default()
        };
        // Tight: the smallest sufficient first, the too-small node last.
        assert_eq!(
            ids_of(set.query(&bindings, &request, None)),
            vec!["four", "eight", "one"]
        );
    }

    /// § 6.9: a usable hint wins outright; an unusable one falls back to
    /// automatic selection and says why; `scan` selects nothing.
    #[test]
    fn index_hints_select_explain_and_scan() {
        let parsed = settings(json!({
            "indexes": { "topo": ["pool", "region"], "zones": ["pool", "zone"] }
        }));
        let bindings = HashMap::from([
            ("pool".to_string(), json!("default")),
            ("region".to_string(), json!("east")),
        ]);
        let hinted = |hint: &str| IndexRequest::default().with_hint(Some(hint));

        // `fit` would lose to `topo` (more partition keys); the hint wins.
        let chosen = parsed
            .select_hinted(&bindings, &hinted("fit"))
            .expect("hint");
        assert_eq!(parsed.indexes[chosen.index].name, "fit");
        assert!(chosen.note.is_none());

        let fallback = parsed
            .select_hinted(&bindings, &hinted("zones"))
            .expect("auto");
        assert_eq!(parsed.indexes[fallback.index].name, "topo");
        assert!(
            fallback
                .note
                .as_deref()
                .unwrap()
                .contains("hint zones unusable: zones needs [zone]"),
            "{:?}",
            fallback.note
        );

        let unknown = parsed
            .select_hinted(&bindings, &hinted("nope"))
            .expect("auto");
        assert!(unknown
            .note
            .as_deref()
            .unwrap()
            .contains("names no declared index"));

        let scan = parsed
            .select_hinted(&bindings, &hinted(SCAN_HINT))
            .unwrap_err();
        assert!(scan.contains("indexHint = \"scan\""), "{scan}");
    }

    #[test]
    fn scan_is_a_reserved_index_name_and_kind_fields_are_checked() {
        for (case, value) in [
            ("reserved", json!({ "indexes": { "scan": ["region"] } })),
            (
                "dims on a plugin kind",
                json!({ "indexes": { "x": { "kind": "islands", "dims": ["resources.a"] } } }),
            ),
            (
                "order on a plugin kind",
                json!({ "indexes": { "x": { "kind": "islands", "order": "tight" } } }),
            ),
            (
                "options on the grid",
                json!({ "indexes": { "x": { "partition_by": ["a"], "options": {} } } }),
            ),
            (
                "bad kind name",
                json!({ "indexes": { "x": { "kind": "../evil" } } }),
            ),
            (
                "bad device dim",
                json!({ "indexes": { "x": ["devices."] } }),
            ),
        ] {
            assert!(
                IndexSettings::from_scheduler_settings(Some(&value), "bin_pack").is_err(),
                "{case} should fail to parse"
            );
        }
        let parsed = settings(json!({
            "indexes": { "x": { "kind": "grid", "partition_by": ["a"] } }
        }));
        assert!(matches!(parsed.indexes[0].kind, IndexKindRef::Grid));
    }

    /// A toy plugin geometry: one cell per node holding its `slot` attr,
    /// ranked by distance from the request's memory in GiB.
    struct SlotKind {
        panic_on_rank: bool,
        broken: std::sync::atomic::AtomicBool,
    }

    impl IndexKind for SlotKind {
        fn name(&self) -> &'static str {
            "slots"
        }
        fn project(&self, node: &Node, _free: &ResourcePool) -> Vec<u16> {
            vec![node_attr(node, "slot").and_then(Value::as_u64).unwrap_or(0) as u16]
        }
        fn rank(&self, cell: &[u16], request: &IndexRequest) -> CellRank {
            if self.panic_on_rank {
                self.broken.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            let want = request.amounts.get("memory").copied().unwrap_or(0.0) / 1024.0;
            let have = f64::from(cell[0]);
            CellRank {
                fits: have >= want,
                distance: have - want,
            }
        }
        fn coverage(&self, request: &IndexRequest) -> usize {
            usize::from(request.amounts.contains_key("memory"))
        }
        fn healthy(&self) -> bool {
            !self.broken.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    fn plugin_set(panic_on_rank: bool) -> IndexSet {
        let mut parsed = settings(json!({
            "index_min_nodes": 1,
            "indexes": {
                "fit": false,
                "slots": { "kind": "slots", "partition_by": ["pool"], "options": { "x": 1 } },
            },
        }));
        assert_eq!(parsed.plugin_kinds(), vec!["slots"]);
        parsed
            .resolve_kinds(|kind, index, options| {
                assert_eq!((kind, index), ("slots", "slots"));
                assert_eq!(options, &json!({ "x": 1 }));
                Ok(Arc::new(SlotKind {
                    panic_on_rank,
                    broken: Default::default(),
                }) as Arc<dyn IndexKind>)
            })
            .expect("resolve");
        let mut set = IndexSet::new(Arc::new(parsed));
        for (id, slot) in [("s1", 1), ("s4", 4), ("s8", 8), ("s2", 2)] {
            let node = make_node(id, "default", json!({ "slot": slot }));
            set.reindex_node(&node, &pool_for(&node));
        }
        set
    }

    /// § 6.7: a plugin kind's projection and rank drive the walk order.
    #[test]
    fn plugin_kinds_project_and_rank_cells() {
        let set = plugin_set(false);
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let request =
            IndexRequest::from_amounts(HashMap::from([("memory".to_string(), 3.0 * 1024.0)]));
        assert_eq!(
            ids_of(set.query(&bindings, &request, None)),
            // Fitting cells by distance, then the tail by distance (the
            // kind's own numbers, so -2 before -1).
            vec!["s4", "s8", "s1", "s2"]
        );
    }

    #[test]
    fn unresolved_and_unhealthy_plugin_kinds_are_never_selected() {
        let unresolved = settings(json!({
            "indexes": { "fit": false, "slots": { "kind": "slots", "partition_by": ["pool"] } }
        }));
        let bindings = HashMap::from([("pool".to_string(), json!("default"))]);
        let reason = unresolved
            .select(&bindings, &IndexRequest::default())
            .unwrap_err();
        assert!(reason.contains("kind slots is not loaded"), "{reason}");

        // A kind that turns unhealthy mid-query (a guarded plugin that
        // panicked) is skipped from the next selection on.
        let set = plugin_set(true);
        let _ = set.query(&bindings, &IndexRequest::default(), None);
        let IndexQueryOutcome::Unusable { reason } =
            set.query(&bindings, &IndexRequest::default(), None)
        else {
            panic!("expected unusable");
        };
        assert!(reason.contains("quarantined"), "{reason}");
    }

    /// § 6.6: `count.per` partition enumeration lists one shard per value
    /// of the per key, matching the other bound keys only.
    #[test]
    fn partitions_enumerate_per_key_values_under_the_bindings() {
        let (set, _) = set_for(
            json!({ "index_min_nodes": 1, "indexes": { "zones": ["pool", "region", "zone"] } }),
            vec![
                make_node("a", "default", json!({ "region": "east", "zone": "1" })),
                make_node("b", "default", json!({ "region": "east", "zone": "2" })),
                make_node("c", "default", json!({ "region": "east", "zone": "2" })),
                make_node("d", "default", json!({ "region": "west", "zone": "3" })),
                make_node("e", "default", json!({ "region": "east" })),
                make_node("f", "other", json!({ "region": "east", "zone": "1" })),
            ],
        );
        let bindings = HashMap::from([
            ("pool".to_string(), json!("default")),
            ("region".to_string(), json!("east")),
        ]);
        let request = IndexRequest::default();
        let enumeration = set
            .enumerate_partitions(&bindings, &request, "zone")
            .expect("served");
        assert_eq!(enumeration.index, "zones");
        let tokens: Vec<&str> = enumeration
            .groups
            .iter()
            .map(|(token, _)| token.as_str())
            .collect();
        assert_eq!(tokens, vec!["\"1\"", "\"2\""]);
        let shard = &enumeration.groups[1].1;
        let mut walked = set
            .walk_shard_verified(enumeration.index_pos, shard, &request, 10, |id| {
                Some(id.to_string())
            })
            .items;
        walked.sort();
        assert_eq!(walked, vec!["b", "c"]);

        // Not a partition key of any index, or an unbound other key: no.
        assert!(set
            .enumerate_partitions(&bindings, &request, "rack")
            .is_none());
        let pool_only = HashMap::from([("pool".to_string(), json!("default"))]);
        assert!(set
            .enumerate_partitions(&pool_only, &request, "zone")
            .is_none());
        assert!(set
            .enumerate_partitions(&bindings, &request, "pool")
            .is_none());
    }
}
