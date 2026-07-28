# Scheduling Indexes

Declarable, ad-hoc indexes over worker nodes that make candidate selection
scale: instead of testing every available node against every task's
constraints on every plan, the scheduler asks a purpose-built structure for
"the nodes this task could land on, best-fit first" and verifies only those.
You declare an index in one line of manager config over *any* node attributes
and resources you care about — including custom ones — and the scheduler
picks the right index per task automatically.

Neither Kubernetes nor Nomad has an equivalent: both filter linearly and
answer scale with sampling (`percentageOfNodesToScore`) or cluster
partitioning. Legion's indexes keep placement quality *and* drop the scan.

## The model

An index is two optional parts:

- **`partition_by`** — a list of exact-match attribute keys. Every distinct
  value-tuple is a shard; a task's equality constraints select exactly one
  shard. Think region/zone/pool/tenant topology.
- **`dims`** — up to six numeric dimensions bucketed into a sparse grid.
  A dim reads either **free resource capacity** (`resources.<name>` —
  fingerprinted capacity minus current allocation usage, for built-in *and*
  custom resources) or any numeric node attribute. A task's resource request
  maps to a grid cell, and candidates come back **nearest-fit first**: nodes
  whose free capacity covers the request, closest match first (or most
  headroom first — see `order`), then everything else.

Partition-only gives a pure inverted index; dims-only gives a pure fit
index; together they give a sharded fit index.

Two invariants make this safe to leave on:

1. **The index proposes; the ledger disposes.** Every candidate an index
   returns is re-verified through the ordinary constraint / permit /
   resource-ledger chain, and the full scan remains the permanent fallback.
   A stale or wrong index can cost a refill or a slightly worse pick —
   never an invalid placement.
2. **Ordering, not filtering.** An untruncated query returns the *whole*
   shard, fit-informed only in its ordering, so index-served planning sees
   exactly the candidate set the scan would have produced (this is pinned
   by tests, and by shadow mode at runtime). Only explicit truncation — the
   per-task candidate budget — narrows what placement sees, and it keeps
   the best-fitting candidates.

## Declaring indexes

Everything lives under `schedulerSettings` in the manager config; changes
take effect at manager restart. The minimal declaration is one line:

```jsonnet
{
  schedulerSettings: {
    indexes: {
      // Shorthand: `resources.*` entries become dims, everything else is a
      // partition key.
      topo: ["pool", "region", "zone"],
      gpu: ["pool", "region", "resources.gpu_vram", "resources.memory"],

      // Kill switch for the built-in fit index (see below).
      fit: false,

      // Full form when the defaults need overriding.
      big_jobs: {
        partition_by: ["pool"],
        dims: [
          { attr: "resources.memory", weight: 2 },            // scale: "log"
          { attr: "gpu_count", scale: "linear", step: 1 },
        ],
        order: "tight",   // "tight" (default under bin_pack) | "loose" (default under spread)
        mode: "on",       // "on" | "shadow" | "off"
      },
    },
    index_min_nodes: 128, // arming threshold; 0 disables indexing entirely
  },
}
```

Defaults do the work:

- Dim entries given as plain strings get `weight: 1` and `scale: "log"`.
- `scale: "log"` buckets by `floor(log2(value + 1))` — unit-independent and
  monotone, the right choice for capacities spanning orders of magnitude.
  `scale: "linear"` with `step` suits small discrete ranges (GPU counts).
- `order` defaults from the scheduling policy: `bin_pack` → `tight`
  (smallest sufficient fit first), `spread` → `loose` (most headroom
  first). This orders *candidate generation*; the policy still scores and
  makes the final choice.
- `mode` defaults to `"on"`. `"shadow"` runs the index alongside the
  authoritative scan and reports disagreements (metrics + warnings) without
  affecting placement — use it to build trust in a new index on a live
  cluster before flipping it on.
- `pool` as a partition key always reads the node's **pool**, the same
  special case permits use — never a node attribute named "pool".

Malformed declarations (unknown fields, more than 6 dims, non-positive
`step`, `true` for anything but `fit`) fail the manager at startup with a
pointed error — a silently-ignored index is the failure mode the explain
surface below exists to prevent.

## The built-in `fit` index

Unless you disable or redefine it, every cluster gets:

```jsonnet
fit: {
  partition_by: ["pool"],
  dims: ["resources.cpu", "resources.memory", "resources.disk"],
}
```

— nearest-fit over free cpu, memory, and disk within the task's pool.
Nodes that don't declare a resource (disk, most commonly — workers do not
fingerprint a schedulable disk resource today, so it exists only where the
operator declares `config_attrs.resources.disk`) project into that dim's
**zero-free bucket**: they match every request that doesn't ask for it and
sort last for requests that do, which is exactly the conclusion the
resource ledger would reach anyway. `allocations` is deliberately not a
dim — its default capacity is unlimited, which buckets uselessly.

Disable it with `indexes: { fit: false }`, or redefine it by declaring your
own index named `fit`.

## Arming: small clusters pay nothing

Indexes participate in planning only when the plan's candidate fleet has at
least `index_min_nodes` available nodes (default **128**). Below the
threshold the scheduler uses the plain scan and the only cost is index
bookkeeping on node/allocation changes — O(1) hash-map work per event, and
none at all if you set `index_min_nodes: 0`, which disables indexing
entirely. Edge clusters never see this feature unless they ask for it.

## Automatic index selection

Per task, the scheduler:

1. Collects the task's **equality bindings**: every `=` constraint, plus
   the pool its permit pins (permits always pin one — `pool=default` by
   default).
2. Keeps indexes whose **every `partition_by` key is bound**. A regex,
   range, or `distinct` rule does not bind a key — such tasks fall back to
   the scan rather than guess.
3. Scores the survivors by how many of the task's **requested resources**
   their dims cover; highest coverage wins, then more partition keys, then
   declaration order. Deterministic, and cheap (runs once per task).
4. Queries the chosen index's shard and re-verifies every candidate with
   the full constraint/permit/driver predicate.

Candidate lists are **truncated** to a budget (`max(64, 8 × remaining
count)`, nearest-fit first) only when nothing downstream needs the full
set — a task with `count.per`, `distinct` constraints, volumes (which can
pin placements), or membership in a multi-task group always gets the whole
shard. If placement exhausts a truncated list while the fleet still has
room, the scheduler widens back to the full scan mid-task and keeps going
(`refills` in the explain output).

## Seeing what happened: `job plan`

`legion job plan` reports, per task, exactly how candidates were generated:

```
Scheduling index:
  web: index topo (on mode, 64 candidates; truncated)
  batch: scan — no usable index: gpu needs [region]
  canary: index fit (shadow mode, 41 candidates; shadow agrees)
```

The same records ride the `--json` envelope (`index_usage`: name, mode,
candidate count, truncation, refills, fallbacks, shadow agreement, and the
reason when no index served). If you declared an index and a task isn't
using it, the `reason` string says which partition key the task fails to
bind. Prometheus counters cover the same ground fleet-wide — see
[metrics.md](metrics.md#scheduling-indexes).

## Maintenance and consistency

Indexes live inside the scheduler's read model and are maintained by the
same write path that mutates nodes and allocations — node joins, heartbeat
attribute changes, availability flips (cordon/drain/uncordon), node
removal, and allocation apply/remove all re-project the affected node in
the same lock. They hold exactly the nodes the scheduler considers
(availability `"available"`), are **never persisted**, and are rebuilt from
the nodes table at startup and on raft failover. Re-projection skips shard
surgery when nothing indexed actually changed, so heartbeat noise on
unindexed attributes costs one comparison.

## Failure modes and diagnostics

- **"My index isn't being used."** `legion job plan` → the task's `reason`
  line. Usual causes: the fleet is below `index_min_nodes`; the task lacks
  an `=` constraint for one of the index's `partition_by` keys; the index
  is `mode: "off"`.
- **Manager refuses to start after editing `indexes`.** The parse error
  names the index and field; fix the declaration — validation is strict on
  purpose.
- **Shadow disagreements** (`legion_scheduler_index_shadow_total{result="disagree"}`
  or `shadow DISAGREES` in `job plan`). The scan stayed authoritative, so
  placements were unaffected — but the index missed eligible nodes and the
  warning log carries a sample of them; please file it.
- **A placement that used to succeed now fails.** Indexes cannot cause
  this: an index-served empty candidate set is always cross-checked against
  the scan before failing, and the scan's verdict wins. If you want to rule
  the feature out anyway, `index_min_nodes: 0` restores pre-index behavior
  byte for byte.

## Known limitations

- **Multi-task group candidate sets** (co-placed groups) are still built by
  scanning; only single-task placement (the overwhelmingly common case) is
  index-served today.
- **Device-set resources** (per-item resources like explicit GPU device
  lists) cannot be dims; the ledger still enforces them on every candidate.
- Selection considers only **equality** constraints for partition binding —
  a range or regex over `region` falls back to the scan even if only one
  region could match.
- Settings changes require a **manager restart**.
- There is no per-task index override in the job spec; selection is always
  automatic.
