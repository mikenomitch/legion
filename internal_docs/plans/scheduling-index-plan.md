# Plan: Declarable Scheduling Indexes (Ad-Hoc Node Filtering and Ranking)

> **Judgement calls:** [`calls/scheduling-index-plan_calls.md`](../calls/scheduling-index-plan_calls.md)

**Status: PLANNED.** Nothing implemented. Deliberately deferred to a later
phase or out of v1 entirely: pluggable index kinds, `count.per` partition
enumeration, device-set (`ResourceValue::Items`) dims, and a task-level
`index_hint` job-spec field.

## Problem Statement

Candidate construction in the scheduler is a full linear scan. Every plan
pass clones **every** available node (`available_nodes`,
`src/manager/scheduler.rs:884`), then per task filters all of them:

```rust
let base_eligible = nodes
    .iter()
    .filter(|node| constraint_set.is_satisfied(node, None) && permit_set.is_permitted(node))
    .collect::<Vec<_>>();   // src/manager/scheduler.rs:1244
```

Each `is_satisfied` call walks every rule, and each rule does JSON-map
lookups across three attribute bags (`node_attr`,
`src/constraints.rs:396`). That is **O(nodes × rules)** per task, with
non-trivial per-node cost (JSON `Value` comparisons, regex, semver). A
1,000-node cluster placing a 10-task job with 3 constraints each does
~30,000 JSON-rule evaluations before a single scoring decision — and again
on every reschedule, rollout tick, and node-drop reconciliation.

The only existing mitigation is `nodes_considered`
(`src/manager/scheduler.rs:2017`), which *samples* a random subset before
scoring. Sampling reduces scoring cost but not filtering cost (the scan
already happened), and it trades placement quality for speed blindly — it
has no idea which nodes are worth considering.

This blocks two standing goals:

- "Performance and scale: prove Legion scales better than K8s, and than
  Nomad" ([`internal_docs/TODO.md`](../TODO.md), Big-picture section).
- "Fix the slowest bits of the scheduler using the benchmarks"
  ([`internal_docs/TODO.md`](../TODO.md), Scheduling & placement).

What a user has to do *today* on a large cluster: shrink the blast radius
by hand — split node fleets into many small pools of managers, or accept
`nodes_considered` sampling and the placement-quality lottery it implies.

## Prior Art

- **Tensorlake's multi-cloud scheduler**
  ([blog](https://www.tensorlake.ai/blog/multi-cloud-scheduling#node-filtering-and-ranking))
  is the direct inspiration. They keep **in-memory reverse indexes and
  Trie structures** over cloud/region/zone for exact filtering, and a
  **4-D spatial index** over CPU/RAM/disk/GPU for nearest-neighbor
  resource fit, chosen because "agent workloads have wider resource-ask
  variance than batch or service workloads." One scheduler manages tens of
  thousands of nodes per region.
  - **Take:** the two-structure insight (exact topology filtering +
    proximity resource fit), heartbeat-maintained in-memory indexes,
    ranking for fragmentation/locality.
  - **Reject:** the *hardcoded* 4-D space. Legion has arbitrary custom
    resources (`src/resources.rs`) and arbitrary node attributes; a fixed
    CPU/RAM/disk/GPU tree contradicts both. Also reject true k-NN: for
    placement you want *nearest among nodes that fit* (a bounded orthant
    query), not nearest point — plain NN happily returns a node that is
    close but too small.
- **Kubernetes** filters all (or a sampled percentage of) nodes per pod:
  `percentageOfNodesToScore` is exactly our `nodes_considered` — sampling,
  not indexing. The kube-scheduler has no attribute index; scale answers
  are "shard into more clusters."
- **Nomad** evaluates feasibility with an iterator stack over all nodes in
  the datacenter/node-pool, short-circuiting per node. Its answer to
  scale is partitioning by datacenter and node pool — the degenerate
  "partition-only" case of what this plan generalizes.

Neither incumbent offers *user-declarable* indexes over *custom*
attributes. That is the differentiator: **declare a nearest-neighbor
index over whatever attributes and resources you care about, in one line
of config** — and get K8s-style sampling-free scale without giving up
placement quality.

## User Scenarios

1. **Zero-config mid-size cluster.** An operator runs 2,000 mixed nodes,
   default pool, no scheduler settings. Placement latency stays flat as
   the fleet grows, because the built-in `fit` index (free cpu / memory /
   disk, partitioned by pool) arms itself automatically above the node
   threshold. The operator never learns the word "index."
2. **GPU fleet with custom resources.** Nodes declare
   `resources: { gpu_vram: 81920 }` in `config_attrs`; tasks request
   `gpu_vram`. The operator adds one line —
   `indexes: { gpu: ["pool", "region", "resources.gpu_vram", "resources.memory"] }`
   — and GPU placements now consider only same-region nodes with enough
   free VRAM, nearest-fit first. `legion job plan` shows
   `"index": "gpu"` per task so they can see it working.
3. **Edge cluster, 12 nodes.** Nothing changes. The default index exists
   but never arms below `index_min_nodes`; candidate construction is the
   same scan as today, and the operator pays zero overhead and sees zero
   new concepts.

## Goals

- Candidate construction cost proportional to *matching* nodes, not
  *total* nodes, for indexed placements.
- Indexes are **declarable ad-hoc** over any attribute or resource —
  multiple indexes, one line each in the easy case.
- A **default-on `fit` index** over free cpu, memory, and disk that can be
  disabled with one setting and never arms on small clusters.
- **Automatic index selection** per task — deterministic, explainable, and
  conservative (anything unindexable falls back to today's scan).
- Placement *semantics* unchanged: an index only proposes candidates; all
  existing hard filters re-verify them.

## Non-Goals

- **No new placement semantics.** With an unbounded candidate budget the
  eligible set is identical to today's; with a bounded budget the index
  changes only *which* eligible nodes reach scoring — the same lever
  `nodes_considered` already pulls, but informed instead of random.
- **No persistence.** Indexes are derived, in-memory state, rebuilt from
  the nodes table at startup and on raft failover.
- **No ANN/spatial libraries.** A quantized sparse grid over `HashMap` is
  enough (§6.4); no new dependencies.
- **No job-spec changes in v1.** Selection is automatic; a task-level
  `index_hint` is deferred until proven necessary.
- **Not a replacement for scoring.** Policies (`bin_pack`, `spread`,
  plugins) and `AffinitySet` still make the final choice among candidates.

## Background — What Exists Today

- **Node model.** Three free-form JSON attribute bags per node —
  `config_attrs`, `fingerprint_attrs`, `dynamic_attrs`
  (`src/structs/node.rs`) — looked up in that order by `node_attr`
  (`src/constraints.rs:396`). `pool` is a top-level column, *not* an
  attribute; permit checking special-cases it (`node_restrictions`,
  `src/constraints.rs:416`).
- **Hard filters.** `ConstraintSet::is_satisfied`
  (`src/constraints.rs:63`) over operators eq/neq/cmp/regex/semver/
  contains/is_set/distinct; `PermitSet::is_permitted`
  (`src/constraints.rs:101`) with implicit `pool=default`. `ConstraintSet`
  exposes `distinct_keys()` (`src/constraints.rs:74`) — precedent for the
  accessor §6.5 needs.
- **Resources.** Task requests are normalized by `ResourcePolicy`
  (`src/resources.rs`; defaults cpu/memory/allocations). Node capacity
  comes from fingerprints plus `config_attrs.resources` for custom
  resources (`ResourcePool::from_node`, `src/manager/scheduler.rs`).
  Fitting is enforced *after* filtering, at selection time, by
  `ResourceLedger` (`src/manager/scheduler.rs:87`,
  `can_fit_task_request` at `:256`), which also handles reservations and
  device-set (`Items`) subset checks. Tasks already request `disk` as a
  custom resource by convention (`configs/jobs/maximal.jsonnet:215`), but
  workers do not fingerprint a schedulable disk resource — only
  stringly-typed totals and pass/fail health booleans
  (`src/worker/node_check/node_checkers/disk.rs`).
- **Candidate flow.** `available_nodes` (`:884`) → per-task
  `base_eligible` scan (`:1244`, inside `build_task_plan` `:1214`) →
  per-placement `eligible_nodes` (`:1578`: permit + distinct + ledger
  fit) → optional `nodes_considered` subsample (`:2017`) → policy
  scoring. Group co-placement builds its own candidate sets from combined
  member constraints in `build_plan`.
- **Read model.** `SchedulingReadModel` (`src/storage/store.rs:110`)
  holds `nodes`, `allocations_by_node`, and `task_configs` behind one
  `RwLock`; `cached_nodes` (`:1492`) is the scheduler's only node source.
  Node join/drop, heartbeats, and `update_node_dynamic`
  (`src/manager/server.rs:4439`) all write through it. This is the
  consistency domain the indexes must live in — and the known
  epoch/cache-refresh fencing gap tracked in
  [`TODO.md`](../TODO.md) (Scheduling & placement) is exactly why the
  index must never be load-bearing for correctness (§6.6).
- **Benchmarks.** `benches/scheduler_bench.rs`,
  `benches/eval_critical_path_bench.rs`, and the scale matrix under
  `benchmarks/scheduler`
  ([docs/testing-and-benchmarks.md](../../docs/testing-and-benchmarks.md)).

## Proposed Design

### 6.1 One shape: partitions × dims

An **index** is declared as:

- `partition_by` — ordered list of exact-match attribute keys. Each
  distinct value-tuple is a shard. (Tensorlake's reverse index / trie.)
- `dims` — list of numeric dimensions, each resolving to either **free
  capacity** of a resource (`resources.*`, computed as
  `ResourcePool::from_node` capacity minus allocation usage from the read
  model) or a numeric node attribute (anything else, via the `node_attr`
  chain). (Tensorlake's spatial index, generalized.)

Both parts are optional: partition-only gives a pure inverted index
(topology, pools, tenancy); dims-only gives a pure fit index; together
they give a sharded fit index. One concept covers everything the blog
post builds with two structures.

**The index proposes; the ledger disposes.** Index output is only a
candidate ordering. Every candidate still passes through today's
`constraint_set.is_satisfied` → `permit_set.is_permitted` → distinct →
`ResourceLedger::can_fit`. A stale or buggy index can cost a refill or a
slightly worse pick — never an invalid placement. This is the invariant
that makes a default-on index acceptable.

### 6.2 Config surface — declaring an index must be one line

Everything lives under the existing free-form `schedulerSettings`
(manager config), keyed by name so jsonnet users can merge and override:

```jsonnet
{
  schedulerSettings: {
    indexes: {
      // Shorthand: a list of attribute keys. Keys starting with
      // "resources." become dims; everything else is a partition key.
      topology: ["region", "zone"],
      gpu: ["pool", "region", "resources.gpu_vram", "resources.memory"],

      // Disable the built-in fit index (see §6.3):
      fit: false,

      // Full form, when the defaults need overriding:
      big_jobs: {
        partition_by: ["pool"],
        dims: [
          { attr: "resources.memory", weight: 2 },
          { attr: "resources.cpu" },
          { attr: "gpu_count", scale: "linear", step: 1 },
        ],
        order: "tight",       // "tight" (bin-pack-ish) | "loose" (spread-ish)
        mode: "on",           // "on" | "shadow" | "off"
      },
    },
    index_min_nodes: 128,     // below this, no index arms (default 128)
  },
}
```

Defaults do the work: dim entries given as strings get `weight: 1` and
`scale: "log"`; `order` defaults from the active policy (`bin_pack` →
`tight`, `spread` → `loose`, anything else → `tight`); `mode` defaults to
`"on"` for user-declared indexes. A non-numeric attribute used as a dim is
rejected at manager start with a clear error; `pool` as a partition key
reads the node's `pool` column (same special case as permits).

The minimal interesting declaration is genuinely one line:
`indexes: { topology: ["region", "zone"] }`.

### 6.3 The built-in `fit` index — default on, with a safety ladder

Answering the "should there be a default?" question: **yes.** Implicitly
declared as:

```jsonnet
fit: {
  partition_by: ["pool"],
  dims: ["resources.cpu", "resources.memory", "resources.disk"],
  // order from policy; mode "shadow" in Phase 2, "on" from Phase 3
}
```

Why default-on is right for Legion:

- The perf claim ("faster and more scalable than Nomad or K8s") has to be
  true *out of the box*, or it is not a differentiator — a tuning flag
  nobody sets is a benchmark asterisk.
- It is safe by construction: candidates are re-verified by the ledger
  (§6.1), and the index never arms below `index_min_nodes` (default 128),
  so edge clusters — the "ease" audience — pay nothing and see nothing.
- Maintenance cost when armed is O(#indexes) hash-map ops per node event
  (§6.6), negligible against the scan it replaces.

Why cpu/memory/**disk** and not more:

- `cpu` and `memory` are universal ledger resources with fingerprinted
  capacity — always meaningful.
- `disk` is requested by tasks today (`maximal.jsonnet:215`) but only
  served by nodes whose operator declared `config_attrs.resources.disk`.
  The grid handles this gracefully: **a node missing a dim's resource
  goes in the zero-free bucket** for that dim. Tasks that don't request
  disk (request = 0) match every bucket including zero-free; tasks that
  do request disk correctly skip nodes that couldn't serve them (the
  ledger's `can_fit` would reject those nodes anyway — the index just
  agrees earlier). So including disk costs nothing on clusters that don't
  use it and works immediately on clusters that do. When workers later
  fingerprint disk as a first-class resource (worth doing, separate
  change; the artifact-cache plan already leans on `resources.disk`
  accounting), the default index picks it up with no config change.
- `allocations` is excluded: its default capacity is infinity
  (`maxAllocations` unset), which buckets uselessly.

Kill switch: `indexes: { fit: false }`. Threshold: `index_min_nodes`.
Rollout ladder: shadow first, on later, with benchmark evidence (§8).

### 6.4 Structure: quantized sparse grid, not a tree

Per index, per partition shard:

```rust
// src/manager/scheduler/index.rs (new module)
type Cell = SmallVec<[u16; 4]>;             // one bucket coord per dim
struct Shard {
    cells: HashMap<Cell, Vec<NodeId>>,      // sparse: only occupied cells
}
struct Index {
    spec: IndexSpec,
    shards: HashMap<PartitionKey, Shard>,   // PartitionKey = value-token tuple
    node_cells: HashMap<NodeId, (PartitionKey, Cell)>,  // for O(1) removal
}
```

- **Bucketing** is static, unit-independent, and monotone:
  `scale: "log"` (default) → `bucket = floor(log2(value + 1))`;
  `scale: "linear", step: s` → `bucket = floor(value / s)`. Static edges
  mean no rebalancing, ever. Log buckets make "6 orders of magnitude
  between MHz and MiB" a non-issue without normalization state.
- **Query.** Compute the request's bucket vector `r`. Matching cells are
  the orthant `c[d] >= r[d]` for all `d` — *nearest that fits*, never
  plain nearest. Iterate the shard's occupied cells (there are at most as
  many as nodes, usually far fewer), keep orthant members, sort by
  weighted L1 bucket distance `Σ weight[d]·(c[d]−r[d])` — ascending for
  `tight`, descending for `loose` — and emit nodes until the candidate
  budget `k = max(64, 8 × remaining_count)` is met.
- **Why a grid and not a k-d tree:** O(1) insert/remove under constant
  node churn (trees degrade and want rebuilds), sparse (empty space is
  free), trivially correct, zero dependencies. The occupied-cell scan is
  deliberately simple — the win over today is doing integer math on
  ≤#nodes cells inside one shard instead of JSON-rule evaluation on
  *every* node; incremental orthant enumeration with early termination is
  a recorded follow-up optimization, not a v1 requirement. Dims are
  capped at 6 per index (validated at parse) to keep cells small.
- Dims resolving to `ResourceValue::Items` (device-ID sets) are rejected
  at parse in v1; the ledger keeps enforcing subset fit for such
  resources downstream.

### 6.5 Automatic index selection — the algorithm, verified against the code

Per task (and per co-placement group), at `build_task_plan` time:

1. **Extract bindings.** Equality bindings come from the task's
   constraints — needs one new accessor,
   `ConstraintSet::equality_bindings() -> Vec<(&str, &Value)>` (only
   `Operator::Eq` rules with scalar values; precedent: `distinct_keys()`,
   `src/constraints.rs:74`). The `pool` binding comes from `PermitSet`
   (which always carries one, defaulting to `pool=default`). For groups,
   use the *intersection* of member bindings.
2. **Filter usable indexes.** An index is usable iff **every**
   `partition_by` key has a binding. (A regex/range/distinct constraint on
   a partition key does not bind it; such tasks fall back — conservative
   by design.)
3. **Score.** `coverage = |dims ∩ requested resource keys|` (request keys
   are already in hand as `task_request`). Pick max coverage; tie-break
   on more partition keys, then declaration order. Deterministic.
4. **Query** the winner's shard for the bound partition tuple; re-verify
   every candidate through the full existing filter chain; hand the
   survivors to the existing `eligible_nodes` / scoring path unchanged.
5. **Refill / fallback.** If placements exhaust candidates before `count`
   is met, re-query with doubled `k`; after two refills, fall back to the
   full scan. No usable index → full scan. The correctness floor is
   always exactly today's behavior.

Feasibility check (the "is this reasonable?" question): steps 1–3 are a
~50-line pure function over data structures that already exist at the
call site (`constraint_set`, `permit_set`, `task_request` are all built
in `build_task_plan`, `src/manager/scheduler.rs:1214`); it is
unit-testable without a store or a cluster, and it runs once per task,
not per node. The one genuine risk is *silent non-use* — a user declares
an index, a task doesn't bind a partition key, and the scan quietly runs
instead. That is an observability problem, not an algorithm problem, and
§6.7 addresses it head-on.

### 6.6 Maintenance and consistency

Indexes live inside `SchedulingReadModel` — a new `indexes: IndexSet`
field — so they share the existing `RwLock` write path and cannot drift
from `nodes`:

- **Node join / drop / availability change** → remove + reinsert the node
  in every index (O(1) each via `node_cells`).
- **Attribute change** (`update_node_dynamic`, heartbeat fingerprints) →
  recompute the node's projected `(PartitionKey, Cell)` per index; skip
  the reinsert when the projection hash is unchanged, so heartbeat noise
  on unindexed attrs costs one hash.
- **Allocation apply / remove** → free-capacity dims changed for the
  affected nodes only; reinsert those nodes in dim-bearing indexes. This
  rides the same code path that already refreshes
  `allocations_by_node` — including whatever resolution lands for the
  epoch/cache-refresh fencing gap already tracked in `TODO.md`; because
  the index only proposes candidates (§6.1), that gap's staleness is
  benign here even before it is fixed.
- **Startup / raft failover** → rebuild from the nodes table; derived
  state is never persisted.
- **Within one plan pass** the index is read-only; in-pass consumption is
  the `ResourceLedger`'s job, exactly as today.
- **Settings changes** (new/removed indexes) take effect at manager
  restart, like other `schedulerSettings`.

### 6.7 Observability and explainability

- `legion job plan` (and its MCP tool) gains a per-task `index` object in
  the JSON envelope: `{ name, mode, partitions_matched, candidates,
  refills, fell_back }` — `null` when no index was usable, with a
  `reason` string (`"no index binds [region]"`). This is the primary
  answer to "why didn't my index get used?".
- Prometheus series on the existing `/metrics` listener:
  `legion_scheduler_index_queries_total{index, outcome}`,
  `legion_scheduler_index_candidates`,
  `legion_scheduler_index_maintenance_seconds`, and in shadow mode
  `legion_scheduler_index_shadow_agreement_ratio{index}`.
- Plan spans get an `index` field alongside the existing stable IDs.

### 6.8 Shadow mode

`mode: "shadow"` runs the scan as the authoritative path and *also* runs
the index query, comparing candidate sets and timings. Disagreements
(index missed an eligible node) log at `warn` with the node and rule that
diverged, and feed the agreement metric. The default `fit` index ships in
shadow (Phase 2) and flips to `on` (Phase 3) only with clean agreement
and benchmark wins — the same "prove it before you trust it" ladder the
raft work used.

## Proto, Struct, Config, and Persistence Changes

- **Proto:** none. No job-spec fields in v1, so `maximal.jsonnet` gains
  nothing (the deferred `index_hint` would).
- **Manager config:** `schedulerSettings.indexes` (map, per §6.2) and
  `schedulerSettings.index_min_nodes` (int, default 128). Parsed and
  validated at manager start (unknown dims types, >6 dims, non-numeric
  dim attrs, and `Items` dims all fail fast with actionable errors).
- **Structs:** `SchedulingReadModel` gains `indexes: IndexSet`
  (`src/storage/store.rs:110`). New module
  `src/manager/scheduler/index.rs` (spec parsing, grid, selection). New
  accessor `ConstraintSet::equality_bindings` and a pool accessor on
  `PermitSet` (`src/constraints.rs`).
- **Persistence:** none — indexes are derived state.
- **Docs:** on shipping, a public page `docs/scheduling-indexes.md` and
  updates to `docs/scheduler-and-eval-loop.md` (candidate-construction
  section) per the plans-README rule that shipped behavior never lives
  only in a plan.

## Implementation Phases

Each phase ships and stands alone.

- **Phase 0 — Measure.** Extend `benches/scheduler_bench.rs` and the
  `benchmarks/scheduler` matrix to isolate candidate-construction time at
  1k / 5k / 10k nodes × {0, 3, 10} constraints, and record the baseline.
  Directly serves the standing TODO item ("fix the slowest bits…"), and
  proves — or disproves — the premise before structure lands. Add the
  AGENTS.md rule that scheduler changes ship with before/after numbers
  (also a standing TODO).
- **Phase 1 — Partition indexes, opt-in.** Inverted index
  (partition-only), settings parsing, automatic selection, refill/scan
  fallback, `job plan` explain field, maintenance hooks, rebuild on
  start. No dims, no default index. Value: topology/pool-heavy clusters
  get the big win; the selection algorithm gets proven on the simple
  structure.
- **Phase 2 — Dims + the grid; default `fit` in shadow.** Bucketing,
  orthant query, free-capacity maintenance on allocation changes, shadow
  machinery and metrics, the built-in `fit` declaration (shadow),
  `fit: false` kill switch, `index_min_nodes` arming.
- **Phase 3 — Default on.** Flip `fit` to `mode: "on"` backed by Phase 0
  vs Phase 2 benchmark deltas and shadow agreement data. Write the public
  docs page; update `scheduler-and-eval-loop.md`.
- **Phase 4 — Backlog (each optional, separately justified):** pluggable
  index kinds via the plugin system (dylib, like scheduler policies);
  `count.per` served by partition enumeration when the per-key is a
  partition key; `Items` dims by cardinality; task-level `index_hint`;
  incremental orthant enumeration with early termination.

## Testing Plan

- **Unit (Phases 1–2):** grid insert/remove/reinsert under churn; bucket
  math per scale including the missing-dim → zero-free rule; orthant
  query tight/loose ordering; `equality_bindings` extraction (map-form,
  list-form, non-Eq, non-scalar); selection scoring and tie-breaks; group
  binding intersection; parse-time validation errors.
- **Equivalence property (Phase 1 onward, the load-bearing test):** for
  generated node fleets and task specs, with unbounded candidate budget,
  the index-produced candidate *set* equals the scan-produced set
  exactly. Runs as a proptest-style loop in `#[cfg(test)]`; shadow mode
  is the same assertion running against production shapes.
- **Integration:** placement outcomes with indexes on vs off across the
  existing scheduler integration suites (same hard-filter guarantees);
  reschedule + node-drop churn with an armed index; raft
  failover rebuild; `job plan` envelope shape (structured-output rules
  per AGENTS.md).
- **Benchmarks:** Phase 0 baseline vs Phase 1/2 on the same matrix;
  a small-cluster (≤64 nodes) guard proving the unarmed path costs
  nothing measurable.
- **Regression:** `concurrent_plans_cannot_oversubscribe_a_node` and the
  epoch-fencing suites unchanged — the index must be invisible to
  conflict safety.

## Rollout and Backward Compatibility

- No migrations, no wire changes, no job-spec changes. Existing clusters
  upgrade with zero behavior change until Phase 3, and even then only
  clusters above `index_min_nodes` see the default index arm.
- Every stage has an off-ramp: `mode: "off"` per index,
  `indexes: { fit: false }` for the default, `index_min_nodes: 0` never
  arms anything… and removing the settings entirely restores today's
  scheduler byte-for-byte, because the scan path remains the permanent
  fallback.
- Multi-raft: indexes exist on whichever manager runs the scheduler
  (leader); rebuilt on election like the rest of the read model.

## Open Questions

- **`index_min_nodes` default (lean: 128).** Phase 0 data should pick the
  point where scan cost visibly beats maintenance cost; 128 is a
  placeholder.
- **Should shadow disagreement auto-disable an index (lean: no, warn +
  metric only)?** Auto-disabling hides bugs; loud warnings surface them.
  Revisit if shadow data shows sustained disagreement in practice.
- **Worker-fingerprinted disk as a built-in resource (lean: yes, but as
  its own small change).** The default index benefits automatically
  (§6.3); coupling it into this plan would drag worker fingerprinting,
  `ResourcePolicy` defaults, and the artifact-cache plan's disk
  accounting into scope.
- **Candidate budget shape (lean: `max(64, 8 × remaining_count)`,
  doubling per refill).** Pure tuning; settle with Phase 2 benchmarks.
  Interaction with `nodes_considered` (which still applies downstream,
  unchanged) is worth a benchmark cell of its own.

## Acceptance Criteria

- **Phase 0:** benchmark output isolates candidate-construction share of
  plan time across the matrix; numbers recorded in
  `benchmarks/scheduler`; AGENTS.md perf rule added.
- **Phase 1:** with `indexes: { topology: [...] }` on a 5k-node fleet,
  indexed placements' candidate-construction time drops proportionally to
  shard selectivity (target ≥10× on a 16-region fleet); equivalence
  property green; `job plan` shows `index` with a truthful `reason` when
  unusable; all existing scheduler tests green untouched.
- **Phase 2:** `fit` shadow agreement ratio 1.0 across the integration
  suites; shadow overhead <5% of plan time; missing-dim nodes provably
  land in zero-free buckets (unit) and never wrongly excluded
  (equivalence property).
- **Phase 3:** default-on beats Phase 0 baseline on 1k+ fleets and is
  within noise below `index_min_nodes`; public docs page exists; kill
  switch verified in an integration test.
