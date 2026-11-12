# Scheduling Indexes

Scheduling indexes help candidate selection scale on large fleets. Without
them, the scheduler tests every available node against every task's
constraints on every plan. With an index, the scheduler asks for "the nodes
this task could land on, best fit first" and verifies only those.

You declare an index in one line of manager config, over any node attributes
and resources you care about, including custom ones. The scheduler picks the
right index for each task automatically.

Kubernetes and Nomad both filter nodes linearly. They handle scale with
sampling (`percentageOfNodesToScore`) or by partitioning the cluster. Legion's
indexes skip the scan while keeping placement quality.

## The model

An index is two optional parts:

- **`partition_by`**: a list of exact-match attribute keys. Every distinct
  tuple of values is a shard. A task's equality constraints select one shard.
  Use this for topology such as region, zone, pool, or tenant.
- **`dims`**: up to six numeric dimensions, bucketed into a sparse grid. A dim
  reads free resource capacity, free devices, or any numeric node attribute.
  Free capacity (`resources.<name>`) is fingerprinted capacity minus current
  allocation usage, for both built-in and custom resources; a custom item
  resource (a list of ids) reads as how many items are free. Free devices
  (`devices.<name>`) count the healthy, unassigned devices of every device
  group the name matches (see [Device dims](#device-dims)). A task's resource
  request maps to a grid cell. Candidates come back nearest fit first: nodes
  whose free capacity covers the request, closest match first (or most
  headroom first, see `order`), then everything else.

Partition-only gives a pure inverted index. Dims-only gives a pure fit index.
Both together give a sharded fit index. The grid is the built-in geometry; a
plugin can replace it (see [Index kinds](#index-kinds)).

Two rules make indexes safe to leave on:

1. **The index proposes, the ledger decides.** Every candidate an index
   returns is re-verified through the ordinary constraint, permit, and
   resource-ledger checks. The full scan is always available as a fallback. A
   stale or wrong index can cost a refill or a slightly worse pick, but never
   an invalid placement.
2. **Indexes order candidates, they don't filter them.** An untruncated query
   returns the whole shard, with fit only affecting the order. Index-served
   planning sees the same candidate set the scan would have produced. Tests
   check this, and so does shadow mode at runtime. Only explicit truncation
   (the per-task candidate budget) narrows what placement sees, and it keeps
   the best-fitting candidates.

## Declaring indexes

All settings live under `schedulerSettings` in the manager config. Changes
take effect when you restart the manager. The minimal declaration is one line:

```nickel
{
  schedulerSettings = {
    indexes = {
      # Shorthand = `resources.*` entries become dims, everything else is a
      # partition key.
      topo = ["pool", "region", "zone"],
      gpu = ["pool", "region", "resources.gpu_vram", "resources.memory"],

      # Kill switch for the built-in fit index (see below).
      fit = false,

      # Full form when the defaults need overriding.
      big_jobs = {
        partition_by = ["pool"],
        dims = [
          { attr = "resources.memory", weight = 2 },            # scale = "log"
          { attr = "gpu_count", scale = "linear", step = 1 },
        ],
        order = "tight",   # "tight" (default under bin_pack) | "loose" (default under spread)
        mode = "on",       # "on" | "shadow" | "off"
      },

      # Free GPUs: an inference task asking for 4 sees nodes with 4+ free first.
      gpus = ["pool", "devices.nvidia/gpu"],

      # A plugin geometry from plugins/indexes/headroom/ (see "Index kinds").
      emptiest = { kind = "headroom", partition_by = ["pool"], options = { resource = "cpu" } },
    },
    index_min_nodes = 128, # arming threshold; 0 disables indexing entirely
  },
}
```

Defaults:

- Dim entries given as plain strings get `weight = 1` and `scale = "log"`.
- `scale = "log"` buckets by `floor(log2(value + 1))`. It is unit-independent
  and monotone, so it suits capacities that span orders of magnitude.
  `scale = "linear"` with `step` suits small discrete ranges, such as GPU
  counts.
- `order` defaults from the scheduling policy: `bin_pack` → `tight`
  (smallest sufficient fit first), `spread` → `loose` (most headroom
  first). This only orders candidate generation. The policy still scores
  candidates and makes the final choice.
- `mode` defaults to `"on"`. `"shadow"` runs the index alongside the
  authoritative scan and reports disagreements (metrics and warnings) without
  affecting placement. Use it to try a new index on a live cluster before you
  turn it on.
- `pool` as a partition key always reads the node's pool, the same special
  case permits use. It never reads a node attribute named "pool".

Malformed declarations fail the manager at startup with a specific error.
These include unknown fields, more than 6 dims, a non-positive `step`, `true`
for anything but `fit`, an index named `scan` (reserved for
[`indexHint`](#hinting-an-index)), `dims` or `order` on a plugin kind,
`options` on the grid, and a plugin kind that is not installed or refuses its
options. Legion rejects them so an index is never silently ignored.

## The built-in `fit` index

Unless you disable or redefine it, every cluster gets:

```nickel
fit = {
  partition_by = ["pool"],
  dims = [
    "resources.cpu", "resources.memory", "resources.disk",
    { attr = "resources.allocations", weight = 0 },
  ],
}
```

This index ranks nodes by nearest fit over free cpu, memory, and disk, within
the task's pool.

The `allocations` dim has weight 0, so it never changes the order. It only
decides whether a cell fits: a node at its `maxAllocations` cap (no free
allocation slot) sorts into the non-fitting tail, after every node that can
take one more allocation. Without it, a full capped node looked idle on cpu
and memory, ranked first under `bin_pack`, and used up the candidate budget on
nodes the resource ledger then refused. Uncapped nodes (unlimited
allocations) and capped nodes with room rank exactly as they would without
the dim.

Nodes that don't declare a resource project into that dim's zero-free bucket.
They match every request that doesn't ask for the resource, and sort last for
requests that do. The resource ledger would reach the same conclusion. Disk is
the most common case. Workers do not fingerprint a schedulable disk resource
today, so disk exists only where the operator declares
`config_attrs.resources.disk`.

Disable it with `indexes = { fit = false }`, or redefine it by declaring your
own index named `fit`.

## Arming: small clusters pay nothing

Indexes take part in planning only when the plan's candidate fleet has at
least `index_min_nodes` available nodes (default 128). Below the threshold,
the scheduler uses the plain scan. The only cost is index bookkeeping on node
and allocation changes, which is O(1) hash-map work per event.

`index_min_nodes = 0` is a special value that turns index queries off, so
every plan uses the scan. It does not stop the bookkeeping. To remove that
cost too, declare no indexes at all (`indexes = { fit = false }` with nothing
else). Small and edge clusters don't use indexes unless you opt in.

## Automatic index selection

Per task, the scheduler:

1. Collects the task's equality bindings: every `=` constraint, plus the
   pool its permit pins. Permits always pin one pool (`pool=default` by
   default).
2. Keeps indexes where every `partition_by` key is bound. A regex, range, or
   `distinct` rule does not bind a key. Those tasks fall back to the scan
   instead of guessing.
3. Scores the remaining indexes by how many of the task's requested resources
   their dims cover (`allocations` never counts: every task requests one).
   Highest coverage wins, then more partition keys, then declaration order.
   This is deterministic and cheap, and runs once per task. A task's
   [`indexHint`](#hinting-an-index) overrides this step when the hinted index
   is usable.
4. Queries the chosen index's shard and re-verifies every candidate with the
   full constraint, permit, and driver checks.

### Truncation

The scheduler truncates candidate lists to a budget (`max(64, 8 × remaining
count)`, nearest fit first). If placement exhausts a truncated list while the
fleet still has room, the scheduler widens back to the full scan once, under
the same filter the list was drawn with, and keeps going (`refills` in the
explain output). What each task shape gets:

- **Plain tasks, `distinct` constraints**: one truncated list. A `distinct`
  task that runs out of distinct values in its list refills like any other.
- **Volumes and stateful tasks**: one truncated list, plus the nodes the task
  must or should use: the node holding an instance's node-local volume, a
  local snapshot's node, and a keyed child's owner node. Those are added to
  the list whenever they pass the task's filters, so truncation never turns
  "place on the pinned node" into "the pinned node is not a candidate".
- **`count.per` tasks**: one truncated list per group. When an index's
  `partition_by` contains the per key and its other keys are bound, the index
  lists its shards for each value of the key and walks each one with that
  group's budget (partition enumeration). Otherwise the task gets an
  untruncated list split into groups. A refill stays inside its group.
  `count.per = "pool"` and `"id"` always take the untruncated path.
- **Multi-task (co-placed) groups**: one truncated list for the whole group,
  from one walk over the members' summed request, verified against every
  member's constraints, permit, and volume drivers. The node the group
  already holds is added to the list. A refill widens under the group's
  filter, so the group still lands on one node. With `count.per`, the group
  gets one list per per-key value, as above.
- **Gangs** always get the whole shard. Deciding that a
  [gang](gang-scheduling.md) does not fit defers its whole plan, and only the
  full list can prove that.

Truncation walks cells best first and includes every cell it reaches in full.
When the budget runs out inside a cell, the rest is a uniform random sample of
that cell, not its lowest node IDs. Nodes in one cell look the same to the
index. A fixed prefix would hand every plan the same few nodes. On a fleet of
identically shaped nodes, that would pack one failure domain to its cap while
the rest sit idle, and `spread` scoring would only ever choose among the
prefix. Expect a truncated candidate list to differ between two otherwise
identical `job plan` runs. The untruncated set never does.

Candidates are verified (constraints, permit, volume drivers) during the walk.
The walk stops one eligible node past the budget. A truncated query therefore
costs about `budget ÷ selectivity` checks, where selectivity is the fraction
of the shard that passes. For ordinary constraints, that cost does not depend
on fleet size. A very selective constraint that is not a partition key of any
index can still cost up to the whole shard. Declare an index partitioned by
that attribute to make it cheap again.

The walk also orders only the cells it reaches. It ranks each occupied cell
once and pops them best first from a heap, so a walk that fills its budget
from the first few cells never sorts the rest.

## Hinting an index

A task can name the index its candidates come from:

```nickel
tasks = {
  infer = {
    runtime = "container",
    indexHint = "gpus",     # an index in schedulerSettings.indexes, or "scan"
    constraints = { region = "us-east" },
    resources = { devices = [{ name = "nvidia/gpu", count = 4 }] },
  },
}
```

- A hinted index that the task can use (not `off`, its plugin kind loaded and
  healthy, every partition key bound) wins over automatic selection.
- `indexHint = "scan"` skips every index and scans. `job plan` reports
  `indexHint = "scan"` as the task's reason.
- A hint naming no declared index, or one the task cannot use, falls back to
  automatic selection. The task's `reason` in `job plan` says why, for example
  `hint gpus unusable: gpus needs [region]`.

A hint is advisory, never an error, because indexes are manager config and
can change without the job changing. An empty or space-padded `indexHint` is
refused at submit. Members of a co-placed group share one walk; it uses the
first member's hint.

## Device dims

`devices.<name>` counts free devices across every device group whose
`vendor/type/model` the name matches: `devices.gpu` counts every GPU,
`devices.nvidia/gpu` NVIDIA's, `devices.nvidia/gpu/A100-SXM4-80GB` one model.
Only healthy, unassigned devices are free. A request's value along the dim is
the summed `count` of its `resources.devices` entries whose names could match
(a request for `gpu` counts against `devices.nvidia/gpu`, one for `amd/gpu`
does not). The default `scale = "log"` buckets counts 1, 2–3, 4–7, 8–15;
use `{ attr = "devices.gpu", scale = "linear", step = 1 }` to tell every
count apart.

A custom item resource (`config_attrs.resources.slots = ["a", "b"]`) works the
same way under `resources.<name>`: free items on the node side, requested
items on the request side.

The device ledger still checks each request's per-device constraints and
assigns exact ids. The dim only orders nodes by how many devices they have
free.

## Index kinds

`kind` selects an index's geometry: how a node maps to a cell and how cells
are ranked for a request. The default, `kind = "grid"`, is the bucketed grid
over `dims` this page describes. Any other name loads a plugin from
`plugins/indexes/<name>/`. Its indexes take `partition_by` and free-form
`options` instead of `dims` and `order`:

```nickel
emptiest = { kind = "headroom", partition_by = ["pool"], options = { resource = "cpu" } },
```

A kind plugin is a Rust dylib that exports an `IndexKindFactory` with
`legion::export_index_kind!`. The factory's `build(index_name, options)`
returns one `IndexKind` per declared index. That kind implements:

- `project(node, free) -> Vec<u16>`: the node's cell, at most 16 values.
  Legion calls it under the scheduler's read-model lock whenever the node or
  its free capacity changes, so it must be cheap and must not block.
- `rank(cell, request) -> CellRank { fits, distance }`: cells that fit come
  before the rest, lower distance first within each.
- `coverage(request)` (optional): how many of the request's resources the
  kind orders by, for automatic selection.

Legion keeps everything else: shards, maintenance, the budgeted walk,
truncation, refills, and re-verification of every candidate. A plugin can make
the candidate order worse, never a placement invalid.
`plugins/indexes/headroom/` is a complete example. It orders nodes by the share
of one resource they have free, which the grid cannot express.

Failure modes:

- **The plugin is missing, built for another Legion, or refuses its
  `options`.** The manager does not start. The error names the index and the
  kind.
- **The plugin panics.** The panic is caught, the kind is quarantined until
  the manager restarts (`legion_plugin_panics_total{kind="indexes"}`), and its
  indexes stop serving. Tasks that used them fall back to automatic selection
  or the scan, and `job plan` gives the reason `kind <name> is quarantined
  after a plugin failure`.

See [plugin-system.md](plugin-system.md) for building and installing dylib
plugins.

## Seeing what happened: `job plan`

`legion job plan` reports how candidates were generated for each task:

```
Scheduling index:
  web: index topo (on mode, 64 candidates; truncated)
  batch: scan — no usable index: gpu needs [region]
  canary: index fit (shadow mode, 41 candidates; shadow agrees)
```

The same records appear in the `--json` envelope (`index_usage`: name, mode,
candidate count, truncation, refills, fallbacks, shadow agreement, and the
reason when no index served). If you declared an index and a task isn't using
it, the `reason` string names the partition key the task fails to bind.
Prometheus counters report the same data across the fleet. See
[metrics.md](metrics.md#scheduling-indexes).

## Performance expectations

Indexes remove the per-task constraint scan, which is the part of planning
that scales with tasks × nodes. The per-plan snapshot is also cheap, because
the read model hands plans `Arc`'d nodes and cached free-capacity pools (see
[state-store.md](state-store.md)). So the index's gain shows up in whole-plan
wall-clock time. On a 5,000-node fleet, a 12-task plan runs about 21% faster
with the default partition index than with the scan (numbers and method:
`benchmarks/scheduler/README.md`). Expect larger gains with more tasks, more
constraints, and more selective shards. Expect none on small fleets, where
indexes never arm.

## Maintenance and consistency

Indexes live inside the scheduler's read model. The same write path that
changes nodes and allocations keeps them up to date. Node joins, heartbeat
attribute changes, availability changes (cordon, drain, uncordon), node
removal, and allocation apply or remove all re-project the affected node under
the same lock.

Indexes hold the same nodes the scheduler considers (availability
`"available"`). They are never persisted. The manager rebuilds them from the
nodes table at startup and on raft failover. Re-projection skips shard updates
when nothing indexed changed, so heartbeat changes to unindexed attributes
cost one comparison.

## Failure modes and diagnostics

- **"My index isn't being used."** Check the task's `reason` line in
  `legion job plan`. Common causes: the fleet is below `index_min_nodes`, the
  task lacks an `=` constraint for one of the index's `partition_by` keys, or
  the index has `mode = "off"`.
- **Manager refuses to start after editing `indexes`.** The parse error
  names the index and field. Fix the declaration.
- **Shadow disagreements** (`legion_scheduler_index_shadow_total{result="disagree"}`
  or `shadow DISAGREES` in `job plan`). The scan stayed authoritative, so
  placements were unaffected. The index did miss eligible nodes, though, and
  the warning log includes a sample of them. Please file a bug.
- **A placement that used to succeed now fails.** Indexes cannot cause this.
  An empty index-served candidate set is always cross-checked against the scan
  before the placement fails, and the scan's result wins. To rule the feature
  out anyway, set `index_min_nodes = 0`, which gives the same behavior as
  having no indexes at all.

## Known limitations

- Selection considers only equality constraints for partition binding. A
  range or regex over `region` falls back to the scan, even if only one region
  could match.
- Partition enumeration serves `count.per` only when the per key is a
  `partition_by` key of an `on` index whose other keys the task binds; other
  `count.per` tasks get an untruncated (index-served or scanned) list split
  into groups.
- Gang tasks never truncate (see [Truncation](#truncation)).
- A device dim counts free devices by name only. Per-device constraints in a
  request (`resources.devices[].constraints`) are checked by the device
  ledger, not the index, so a node with enough free devices that fail those
  constraints still ranks as fitting.
- Settings changes, including plugin kinds and their `options`, require a
  manager restart.
