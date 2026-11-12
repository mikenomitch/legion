# Plan: Candidates and Indexes — Closing the Scheduling-Index Backlog

> **Judgement calls:** [`calls/candidates-and-indexes-plan_calls.md`](../calls/candidates-and-indexes-plan_calls.md)

**Status: SHIPPED.** All six "Candidates and indexes" items from
`internal_docs/TODO.md` (2026-11-12). Nothing in the scope below was deferred.
Gang tasks keep the whole candidate list on purpose; see §6.4.

This plan picks up where two earlier plans stopped:
[`scheduling-index-plan.md`](scheduling-index-plan.md) (Phase 4 backlog and
calls 10/11) and [`plan-path-fleet-scaling-plan.md`](plan-path-fleet-scaling-plan.md)
(§13.5, the in-place rollout exclusion set).

## 1. Problem

The default `fit` index made candidate construction O(budget) for the common
single-task placement. Everything around it still has edges where planning is
O(fleet), where the index ranks the wrong nodes first, or where a placement
reads state that is not fenced:

1. **`maxAllocations` is invisible to `fit`.** A node capped at N allocations
   that already holds N still projects into a fitting cell, because the index
   sees only cpu, memory, and disk. Under `bin_pack` those full nodes rank
   first (they look lightly loaded on cpu and memory), and the candidate budget
   fills with nodes the ledger then refuses.
2. **The node-namespace check costs a second attribute walk per node per
   task.** `PermitSet::is_permitted` looks up `restriction` and then
   `namespaces` on `config_attrs` separately. Measured as 11.0 → 12.2 ms at
   10k nodes when the check landed (legion-apps calls 25).
3. **In-place rollout ticks build an O(fleet) exclusion set.** To keep a
   replacement on its old nodes, `build_in_place_exclusions` lists every
   available node that is *not* allowed, and the plan then copies the node set
   without them.
4. **Truncation-ineligible tasks still scan the fleet.** `count.per`,
   `distinct`, volume-bearing and stateful tasks, and multi-task groups all
   verify every node of the shard, which with the default `fit` index is the
   whole pool.
5. **The scheduling-index backlog.** Group co-placement does not use indexes
   (calls 10); index kinds cannot be extended; `count.per` cannot be served
   from partitions; device-set resources cannot be dims; there is no task-level
   `index_hint` (calls 11); and every truncated query sorts every occupied
   cell of the shard even when the walk stops after the first few.
6. **Volume pins are read mid-plan with no fence.** The planner reads a
   pending instance's node-local volume pin from SQLite, outside the snapshot
   the epochs fence. A `volume delete --force` between that read and the apply
   still steers the placement to the old node.

## 2. Prior art

- **Kubernetes** filters every node (`PreFilter`/`Filter`), with
  `percentageOfNodesToScore` as the only scale lever. Topology spread
  (`maxSkew` per `topologyKey`) runs over the full filtered list. There is no
  index and no hint: the scheduler profile is the only per-pod override
  (`schedulerName`). We take the per-workload override idea (§6.6) but keep it
  advisory.
- **Nomad** iterates a randomized node list with a limit (`LimitIterator`) and
  `spread` blocks over attributes; `distinct_property` is a filter on the same
  iterator, so it costs nothing extra once the iterator is bounded — the
  property we copy for `distinct` (§6.4). Nomad's `max_allocs`-style limits are
  checked in the feasibility chain, after candidate selection; it has the same
  "full nodes eat the limit" failure we fix in §6.1.
- **Tensorlake's indexes** (the inspiration for the original plan) are
  pluggable structures behind one query interface; we take that shape for
  index kinds (§6.7) but keep maintenance and re-verification in the host.
- **Borg/Omega** use equivalence classes so tasks with identical requirements
  share one feasibility pass — the same idea as serving a whole multi-task group
  from one index walk (§6.5).

## 3. Jobs to be done

**Cluster operator**
- When I cap nodes at a number of allocations, I want the scheduler to stop
  offering full nodes first, so packing does not slow down as the fleet fills.
- When I run a large fleet with spread-by-zone workloads, I want placement to
  stay as fast as it is for plain workloads, so a common jobspec shape does not
  fall off a performance cliff.
- When I have a placement structure the built-in grid cannot express (GPU
  islands, a NUMA layout), I want to plug in my own ordering without forking
  Legion, and I want a broken plugin to degrade placement rather than stop it.

**Job author**
- When the automatic index choice is wrong for my task, I want to name the
  index it should use (or ask for a full scan), so I can fix placement without
  changing cluster config.
- When my task asks for GPUs, I want the GPU index to rank nodes by how many
  free GPUs they have, so tasks needing four GPUs are not offered nodes with one.

**On-call responder**
- When I force-delete a volume, I want the pending instance that pointed at it
  not to be placed onto that node on the strength of a pin that no longer
  exists.

**Autonomous agent**
- When I dry-run a plan, I want the explain output to say which index served
  each task and why a hint was ignored, so I can fix the jobspec without a
  human.

### User scenarios

1. A 10,000-node fleet, `maxAllocations = 10`, `bin_pack`. Halfway full, a
   burst of one-replica jobs: candidates come from nodes with free slots, and
   `refills` in `job plan` stays at 0.
2. A `count = { num = 2, per = "zone" }` service on a fleet with an index
   partitioned by `["pool", "zone"]`: each zone's two replicas come from a
   budgeted walk of that zone's shard, not a scan of the pool.
3. An inference task with `indexHint = "gpu"` and
   `resources.devices = [{ name = "nvidia/gpu", count = 4 }]` on a fleet whose
   `gpu` index has the dim `devices.nvidia/gpu`: nodes with four or more free
   GPUs come first.

## 4. Goals and non-goals

**Goals**
- Each TODO item above closes in full, with a test that pins its behavior.
- Placement semantics never change: indexes propose, the ledger and the
  apply recheck dispose (scheduling-index calls 1). Every new truncation keeps
  the refill-to-scan safety net.
- New surfaces are one line in the easy case.

**Non-goals**
- Gang tasks stay untruncated: proving "the gang does not fit" needs every
  eligible node (docs/gang-scheduling.md). Not on the TODO item, and the reason
  is inherent, not a cost.
- No live reload of index settings or plugin kinds (scheduling-index calls 13).
- No new persistence for indexes. `indexHint` is a task column like any other
  task field; index state stays derived.

## 5. What exists today

- `crates/common/src/scheduling_index.rs`: `IndexSettings` (parse, `select`),
  `IndexSet` (per-index shards of cells), `select_cells` (sorts every occupied
  cell), `walk_verified` (budgeted, verify-as-you-walk), `default_fit_spec`.
- `crates/manager/src/manager/scheduler.rs`: `build_task_plan` decides
  `allow_truncation` (`count.per`, `distinct`, volumes, stateful, gang,
  multi-task group all disable it); `indexed_base_eligible` runs the walk or the
  full query; `build_plan` builds `group_candidates` with a full scan; the
  `count.per` loop groups `base_eligible` with `group_nodes_by_attr`;
  `place_task_allocations` refills a truncated list from the full scan only
  when `group_key.is_none()`.
- `crates/common/src/constraints.rs`: `PermitSet::is_permitted` =
  `permits_restrictions` + `accepts_namespace`, two `config_attrs` lookups.
- `crates/manager/src/manager/rollout.rs`: `build_in_place_exclusions` returns
  `available − allowed`; `Scheduler::plan_and_apply_internal` filters the node
  set with `AvailableNodes::without`.
- `Store::pinned_placements_for_task` reads volume pins from SQLite mid-plan;
  the apply (`Store::apply_plan_submission`) re-checks node availability,
  constraints, capacity, ports, and devices, but not the pin.

## 6. Design

### 6.1 `fit` sees `maxAllocations` (TODO 1)

The built-in `fit` index gains a fourth dim, `resources.allocations`, with
**weight 0**. The `allocations` resource already exists (every task requests
one, `maxAllocations` sets the node's capacity, uncapped is infinite). Weight 0
means the dim takes part in the orthant test only: a capped node with no free
slot falls into the non-fitting tail, and a node with room ranks exactly as
before. Selection coverage ignores `allocations`, because every task requests
it and it would otherwise make `fit` outrank a custom index that covers the
task's real resources.

```nickel
fit = {
  partition_by = ["pool"],
  dims = ["resources.cpu", "resources.memory", "resources.disk",
          { attr = "resources.allocations", weight = 0 }],
}
```

### 6.2 One attribute walk for permit and namespaces (TODO 2)

The plan's cached node set (`AvailableNodes`) keeps a per-node flag, filled
lazily on first use: the node declares no restriction block and no
`namespaces` list. `PermitSet::is_permitted_flagged` judges such a node from
its pool string with no attribute lookups; the scan, the index walk's verify,
refills, and pinned-node top-ups all use it. `is_permitted` itself fetches
`config_attrs` once for both lookups. Re-measured with
`scheduler_bench -- candidate_construction` before and after (calls 7: no
regression; the saving is below this box's noise).

### 6.3 In-place rollouts pass an allow-list (TODO 3)

`Scheduler::plan_and_apply_with_targets_and_scope` takes a `NodeScope`:
`All`, `Exclude(&set)` (reschedules), or `Only(&set)` (in-place rollouts).
`AvailableNodes::only(&set)` builds the node set from the allowed ids by
lookup, O(|allowed|). `build_in_place_allow_list` intersects the old nodes with
the cached available set by lookup instead of materializing
`available_node_ids()`.

### 6.4 Truncation for `distinct`, volumes, and stateful tasks (TODO 4)

- **`distinct`** truncates like any task. The tracker is checked per placement
  on the candidate list; a truncated list that runs out of distinct values
  runs dry, and the existing refill widens to the full scan once. Correct by the
  same argument as every other truncation.
- **Volumes and stateful tasks** truncate too, but the nodes a placement *must*
  or *should* use — volume pins, local-snapshot restore pins, a keyed child's
  owner node — are loaded before the scoring cache is built and appended to the
  truncated list when they pass the task's filters. A pinned placement
  therefore always sees its node, truncated or not.
- **Gang tasks** keep the whole list (non-goal).

### 6.5 Multi-task groups through the index (TODO 4, backlog: group co-placement)

A multi-task group places every member on one node, so its candidates must pass
every member's filters and hold every member's request. The group's index
query uses the union of members' equality bindings (a key bound to two values
by two members means no node qualifies; the scan path says the same) and the
summed request, and walks with the group predicate (every member's constraints
and permit). The list truncates at `candidate_budget(sum of members' counts)`.
Nodes the group is already assigned to (existing allocations) are appended.
When a member's list runs dry, the refill widens to a scan **with the group
predicate** (and the `count.per` token, below), so pinning to one node still
holds.

### 6.6 `count.per` by partition enumeration (TODO 4, backlog)

When a task (or group) has `count.per = K`, an index whose `partition_by`
contains `K` and whose other keys are all bound serves it: the index enumerates
its shards whose other key tokens match the bindings, one shard per value of
`K`, and each group's candidates come from a budgeted walk of its shard. Groups
with no node are simply not enumerated, as with `group_nodes_by_attr`. The
reschedule count for a group reads the job's existing allocations by their
node's `K` token, O(job's allocations), instead of intersecting with a full
group list. A refill inside a group widens to the scan filtered to that
group's token. `K = "pool"` or `"id"` (special-cased differently by the index
and the grouping) fall back to the scan.

### 6.7 Pluggable index kinds (backlog)

An index declaration gains `kind` (default `"grid"`, the built-in) and
`options` (free-form, passed to the kind):

```nickel
indexes = {
  islands = { kind = "nvlink_islands", partition_by = ["pool"], options = { min_island = 4 } },
}
```

A kind owns only geometry. It projects a node to a cell and ranks a cell for a
request; Legion keeps shards, maintenance under the read-model lock, the
walk, truncation, refill, and re-verification. A kind therefore cannot cause an
invalid placement, only a worse order. The contract is in `legion-common`
(`IndexKind`, `IndexKindFactory`); a dylib under `plugins/indexes/<name>/`
exports a factory with `legion::export_index_kind!`. Calls into a plugin are
guarded: a panic quarantines the kind, and its index then answers `Unusable`,
so planning uses the scan.

### 6.8 Device-set dims (backlog)

A dim may read device sets:
- `resources.<name>` where the node's free value is an item set (a custom item
  resource) buckets by the number of free items, and a request's item set by
  its size;
- `devices.<name>` (`gpu`, `nvidia/gpu`, `nvidia/gpu/A100-SXM4-80GB`) buckets
  by the number of free healthy devices in every group the name matches, and a
  request by the summed `count` of its `resources.devices` entries whose names
  could match.

Both count toward selection coverage.

### 6.9 Task-level `indexHint` (backlog)

```nickel
tasks = { infer = { indexHint = "gpu", ... } }
```

The named index is used when it is usable for the task (not `off`, every
partition key bound). `indexHint = "scan"` forces the full scan. An unknown or
unusable hint falls back to automatic selection, and `job plan` says so in the
task's `reason` (`hint gpu unusable: gpu needs [region]`). `scan` is therefore a
reserved index name. Advisory, never an error: a hint is a performance
statement, and failing a submit because an operator renamed an index would
turn config drift into an outage.

### 6.10 Incremental orthant enumeration (backlog)

`select_cells` ranks every occupied cell once (O(C)) and hands the walk a
binary heap instead of a sorted vector. The walk pops cells best-first and
stops when the budget and the one-past check are satisfied, O(C + k log C)
instead of O(C log C). The full query still drains the heap, so its output is
unchanged.

### 6.11 Volume pins are fenced at apply (TODO 6)

`PlannedAllocation` and the serialized `PlanSubmissionPlacement` carry
`volume_pin` (the instance whose node-local volume chose the node). The apply
re-reads that pin inside its write transaction; a pin that moved or vanished
skips the candidate like any other "the world moved" class, so the plan is
refused and the eval re-plans against current pins.

## 7. Proto, struct, config, and persistence changes

- `TaskConfig.index_hint: Option<String>` (`indexHint`), column
  `task_config.index_hint` (migration), job schema, `configs/jobs/maximal.ncl`.
- `schedulerSettings.indexes.<name>.kind` / `.options`; dims accept
  `devices.<name>`; the name `scan` is reserved.
- `PlanSubmissionPlacement.volume_pin` (serde-default, so old rows parse).
- Plugin kind directory `plugins/indexes/`, manifest kind `indexes`.
- No proto message changes; `TaskIndexUsage.reason` carries hint diagnostics.

## 8. Phases

All shipped together; each item is independent and could ship alone:
6.1, 6.2, 6.3, 6.11 are self-contained; 6.4–6.6 share the refill scope; 6.7–6.10
are independent extensions of `scheduling_index.rs`.

## 9. Testing

- `scheduling_index.rs` units: zero-weight dim fit test, coverage ignores
  `allocations`, heap walk equals sorted walk, partition enumeration, device and
  item dims, hint selection, plugin kind ordering and panic quarantine, `scan`
  reserved.
- Scheduler units: a capped full node is not a truncated candidate; `distinct`
  and volume-pinned tasks truncate and still place on the pin; `count.per` via
  enumeration places per group and reschedules only the missing group; a group
  served by the index co-places; refills honor group and per-token scope;
  `indexHint = "scan"` and an unusable hint.
- Rollout unit: the in-place allow-list equals the old exclusion semantics.
- Store unit: an apply with a stale volume pin is refused.
- `scheduler_bench -- candidate_construction` before/after for 6.2.

## 10. Rollout

Default changes: `fit` gains the zero-weight `allocations` dim, and more task
shapes truncate. Both only reorder or bound candidates; the scan stays the
fallback and `index_min_nodes = 0` still turns everything off. The migration
adds a nullable column. No compatibility shims (AGENTS.md).

## 11. Open questions

None blocking. The candidate budget for groups and `count.per` groups reuses
`candidate_budget`; tune with rig data if refills show up.

## 12. Acceptance criteria

- Each item's tests above are green; `cargo test --all --locked`, fmt, clippy
  green.
- `job plan --json` shows index usage for group members and `count.per` tasks,
  and hint diagnostics in `reason`.
- `docs/scheduling-indexes.md` documents every new surface and drops the
  limitations this plan removes.
