# Judgement Calls — Candidates and Indexes

Companion to
[`plans/candidates-and-indexes-plan.md`](../plans/candidates-and-indexes-plan.md).
Entries are ranked by project impact, highest first.

## 1. Every task shape but a gang truncates, behind one refill scope and a pinned-node top-up

**Call.** `distinct`, volume-bearing, stateful, `count.per`, and multi-task
group placements all take a budgeted candidate list now. Each list carries the
predicate it was drawn with (`CandidateFilter`: one task's filters, or every
group member's, narrowed to one `count.per` token), and a list that runs dry
refills from a scan under that predicate. A truncated list gets the nodes a
placement must or should use added back (volume pins, local-snapshot pins, a
keyed child's owner, the node a co-placed group already holds) when they pass
the predicate. Only gangs keep the whole list.

**Alternatives.** (a) Keep the v1 rule ("anything that might need the full set
scans") and close the TODO as a non-goal. (b) Truncate, but on a dry list
refill with the task's own filters only, as the single-task refill did. (c)
Read pins and enlarge the budget instead of topping up.

**Trade-off.** A truncated `distinct` task can refill where the scan would have
found diversity at once (one extra scan, only when the budget ran out of
distinct values). The top-up adds a few by-id lookups per placement call.

**Why (performance, then simplicity).** The default `fit` index partitions by
pool, so on a one-pool fleet every truncation-ineligible task verified the
whole fleet per plan, which is exactly the O(fleet) term the fleet-scaling plan
exists to remove. (a) leaves the most common stateful and spread-by-zone shapes
on that path. (b) breaks correctness: a group member refilled with its own
filters can pick a node another member's constraints refuse, and a `count.per`
group refilled fleet-wide places outside its group. One predicate type shared
by the scan, the walk, the refill, and the top-up removes that class of bug.
(c) still misses a pin whenever the budget is smaller than the shard. Gangs stay
untruncated because "the gang does not fit" defers the whole plan, and only a
complete list can prove it.

## 2. A plugin index kind owns geometry only: `project` and `rank`

**Call.** `IndexKind` is stateless: `project(node, free) -> cell`,
`rank(cell, request) -> {fits, distance}`, optional `coverage`. Legion keeps the
shards, maintenance under the read-model lock, the budgeted walk, boundary-cell
sampling, refill, partition enumeration, and re-verification.

```nickel
emptiest = { kind = "headroom", partition_by = ["pool"], options = { resource = "cpu" } },
```

**Alternatives.** A full structure plugin (`upsert`, `remove`, `query ->
ranked ids`), the Tensorlake shape the original plan pointed at.

**Trade-off.** A kind cannot use a structure the grid's "cells in a map"
model cannot express (a true k-d tree, a learned index). It also pays one
`rank` call per occupied cell per query.

**Why (extensibility with safety).** The invariant that makes a default-on index
defensible is "the index proposes, the ledger disposes" (scheduling-index calls
1). A full-structure plugin would own maintenance under the read-model lock and
truncation fairness (calls 14 of that plan), two places a bug costs placements
or stalls heartbeats. A geometry plugin can only reorder. Keeping it stateless
also keeps the read model `Clone` (the plugin is an `Arc`), and it gets the
heap walk, enumeration, and shadow mode for free.

## 3. `indexHint` is advisory, and `scan` is a reserved index name

**Call.** A hint names an index or `"scan"`. A usable hinted index wins, `scan`
skips indexes, and an unknown or unusable hint falls back to automatic
selection with the reason in `job plan` (`hint topo unusable: topo needs
[region]`). Submit refuses only an empty or space-padded hint. An index may not
be named `scan`.

**Alternatives.** Fail the submit (or the plan) when the hint names no index
or one the task cannot use. Use a separate field (`indexScan = true`) for the
scan instead of a reserved name.

**Trade-off.** A typo in a hint is silent until someone reads `job plan`.

**Why (simplicity for the job author, safety for the operator).** Indexes are
manager config; jobs outlive config changes. A hard error would turn an
operator renaming an index into failed deploys across every job that hinted it,
for a field that is only ever a performance statement. One field with one
reserved value is easier to type and document than two fields.

## 4. `fit` sees `maxAllocations` through a zero-weight `allocations` dim, and coverage ignores `allocations`

**Call.** The built-in `fit` index gains `{ attr = "resources.allocations",
weight = 0 }`. Weight 0 makes the dim a pure fit test, so a node at its cap
falls into the non-fitting tail while every other node keeps its order.
Selection coverage never counts `allocations`.

**Alternatives.** (a) A weighted fourth dim. Uncapped nodes are infinite (bucket
64), so they would sit 63 buckets from every request and rank after any capped
node with room. That reorders the whole fleet. (b) Filter capped-full nodes out
of the walk. That breaks "indexes order, they don't filter" (the equivalence
property). (c) A boolean "cap reached" flag outside the dims.

**Trade-off.** The cell vector grows by one `u16` per node, and the default
`fit` reads as four dims in docs.

**Why (performance, then simplicity).** (b) breaks the untruncated-set
equivalence the tests pin down. (c) adds a second mechanism next to dims. Weight 0
was already valid in the dim grammar, so the fix is declarative and visible in
the documented default. Coverage has to ignore `allocations` because every task
requests it. Counting it would make `fit` outrank a custom index that covers
the task's real resources (the `gpu` example in the docs).

## 5. Partition enumeration serves `count.per` only when the per key is a partition key, never for `pool` or `id`

**Call.** An `on` index whose `partition_by` contains the per key, with every
other key bound, enumerates its shards per value and walks each with that
group's budget. Otherwise the task gets an untruncated list split by token.
`count.per = "pool"` / `"id"` always take the untruncated path. Reschedule
counts per group read the job's allocations by their node's token.

**Alternatives.** Enumerate by scanning the shard and grouping (no index
requirement), or make the per key bindable by any index.

**Trade-off.** A `count.per` task on a fleet without a matching index still
verifies the shard. That is the documented limitation and the reason to
declare `zones = ["pool", "zone"]`.

**Why (predictability).** The same strict-binding rule as selection
(scheduling-index calls 9): either the index answers the question exactly, or
the scan does. `pool` and `id` are grouped by rules the partition tokens do not
share. `pool` the partition reads the column, `count.per` the attribute, and
`id` is grouped raw. Enumerating them would split groups differently from the
scan.

*Execution.* A co-placed group with `count.per` now takes its tokens from the
group's own candidate lists. A token where one member had an eligible node but
no node passed every member's filters used to fail the plan ("No eligible nodes
for group …"). It is now simply not a group. That is placement where possible,
the same posture as partial placement elsewhere.

## 6. Volume pins are fenced by re-reading them at apply, not by an epoch

**Call.** `PlanSubmissionPlacement.volume_pin` names the instance whose
node-local volume chose the node. The apply walk re-reads the pin in its
transaction and skips the candidate when the pin moved or vanished, so the plan
is refused and the eval re-plans.

**Alternatives.** (a) Put volume rows under a scope epoch the plan samples. (b)
Let the binder fall back to any free slot (today's behavior: the instance lands
on the old node with no data).

**Trade-off.** One indexed point read per pinned placement at apply.

**Why (correctness at minimal cost).** (a) adds an epoch dimension every volume
mutation must bump and every plan must sample, for a race that needs a forced
delete mid-plan. (b) is the bug. The re-read uses the same query the binder
already uses (`pinned_node_for_instance_tx`), so planner and binder cannot
disagree about what a pin is.

## 7. The namespace check is removed by a per-node flag in the cached node set, not only by folding the lookups

**Call.** `AvailableNodes` caches, per node, whether it declares a non-empty
`restriction` block or any `namespaces` list (`node_is_unrestricted`). The
flag is computed lazily, on first use per node per set, into a relaxed
`AtomicU8`. Hot paths call `PermitSet::is_permitted_flagged`, which judges an
unrestricted node from its pool string with no attribute lookups.
`is_permitted` itself also fetches `config_attrs` once for both lookups.

**Alternatives.** Fold the two lookups into one `config_attrs` walk only (the
TODO's second option).

**Trade-off.** One byte per node per node-set build, and every caller that has
a node set must pass the flag. The first version computed every flag eagerly
when the set was built. `candidate_construction/indexes_on` (which builds a
fresh set per iteration) paid that for all 10k nodes and went from 9.2 to
10.9 ms, so the flag became lazy. A budgeted walk pays only for the nodes it
visits; a scan pays once per node and every later task reuses it. A
`OnceLock` was tried first and dropped for a plain atomic: two planners
filling one slot compute the same value, so the race is harmless.

**Why (performance).** Folding still costs two map lookups per node per task,
and the measured regression (11.0 → 12.2 ms at 10k nodes) was the second one.
The flag removes both lookups, the original `restriction` one included, for the
common node. A unit test pins `is_permitted_flagged == is_permitted` across
node and permit shapes.

*Measured* (`scheduler_bench -- 'candidate_construction/indexes_(off|on)/nodes_10000$'`,
release, one 4-core cloud box, change / base / change run back to back):

| Cell | change | base | change |
| --- | --- | --- | --- |
| `indexes_off` (scan) | 31.3 ms | 26.1 ms | 25.7 ms |
| `indexes_on` | 9.8 ms | 9.6 ms | 9.5 ms |

Run-to-run spread on this box is about 15% (earlier base runs: 26.1, 26.0,
25.6 ms), larger than the 1.2 ms the namespace lookup was measured at. So the
saving is not resolvable here; what the numbers do show is no regression on
either path. The rig or a quieter box is where it would show.

## 8. Device dims are `devices.<name>` and match by `DeviceName::compatible`

**Call.** A new dim prefix, `devices.<name>`, counts free devices across every
group the name matches. A request counts the demands whose names could match. A
`resources.<name>` dim over an item-set resource counts free items.

**Alternatives.** Make operators name the pool key (`resources.device:nvidia/gpu/A100`),
one dim per model.

**Trade-off.** The dim knows names only. A request's per-device constraints are
still the device ledger's to check (documented limitation).

**Why (ease).** Operators think "GPUs", not pool keys, and a fleet with three
GPU models would otherwise need three dims for one question. `compatible` is
the rule device quotas already use, so a quota and an index agree on what
`gpu` means.

## 9. A co-placed group's candidates also require every member's volume drivers

**Call.** The group predicate includes the union of members' volume drivers.

**Alternatives.** Constraints and permit only, as the old group scan did.

**Trade-off.** A group whose member needs a driver no node has now fails at
plan time rather than at mount time on the worker. That is a behavior change,
and a stricter one.

**Why (correctness).** A group lands every member on one node. The old group scan
let a member land on a node that cannot mount its volume, which the single-task
path has refused since volume drivers existed.

## 10. In-place rollouts pass a `NodeScope::Only` allow-list

**Call.** `Scheduler::plan_and_apply_with_targets_and_scope(…, NodeScope)` with
`All | Exclude | Only`, and `AvailableNodes::only` built by id lookup.

**Alternatives.** Keep `Exclude` and build the exclusion lazily, or add an
`allowed_nodes` parameter beside `excluded_nodes`.

**Why (simplicity).** One enum replaces an option that could only exclude, and
the two shapes cannot both be passed. The rename breaks the old method name
on purpose (AGENTS.md: no compatibility shims).

## 11. The heap walk changes ordering cost, not order

**Call.** `select_cells` ranks each occupied cell once and pops from a binary
heap. The full query drains it, so its output is identical, and the budgeted walk
stops early. A unit test compares the heap order with a brute-force sort.

**Why (performance).** O(C + k log C) instead of O(C log C) per truncated query.
There was no alternative worth recording beyond leaving the sort.

## 12. A co-placed group's walk uses the first member's hint

**Call.** The members' requests are summed into one `IndexRequest`. Its hint is
the first member's (in declaration order) that sets one.

**Alternatives.** Refuse conflicting hints within a group.

**Why (simplicity).** A hint is advisory (entry 3). Refusing a submit over two
advisory fields disagreeing would be the only hard error the field can raise.
Documented in the public page.
