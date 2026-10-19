# Judgement calls: plan-path-fleet-scaling-plan

Plan: [`../plans/plan-path-fleet-scaling-plan.md`](../plans/plan-path-fleet-scaling-plan.md)

Entries are ranked by project impact, highest first. The number is the
rank. Entries 1-4 and 8-10 are design-time calls. Entries 7 and 12 are
deviations made while executing Phases 1 and 2 (2026-10-09). Entries 5, 6,
11, 13, and 14 are Phase 4 (2026-10-10), and entry 11 supersedes entry 12.

## 1. The plan snapshot becomes O(1) through persistent maps, not per-worker incremental snapshots

**Call.** `SchedulingReadModel`'s plan-facing maps (`allocations_by_node`,
`node_pools`, `scope_epochs`, `allocations_by_job`) become persistent hash
maps (`imbl`). A plan's snapshot is a few root-pointer clones under the
read guard, and writers path-copy only what they touch. The available-node
list is handed out as its `Arc` instead of a cloned `Vec`.

**Alternatives.**
(a) Kubernetes' model: each eval thread keeps a private snapshot and
patches it from a generation-ordered change journal before each plan, so
a plan costs O(changes since its last plan). This is the proven design at
the largest scale anyone runs a scheduler.
(b) A dependency-free sharded copy-on-write map: 256 `Arc<HashMap>`
shards, O(256) snapshot, a write copies one shard.
(c) Keep the clone and accept the linear term, as
[plan-path-scale-levers calls #2](plan-path-scale-levers-plan_calls.md)
did.

**Trade-off.** A new dependency, and persistent-map lookups are a few
times slower than `HashMap` lookups. Plans read a few hundred entries
per eval after §6.2, so the lookup cost is noise. Old versions stay alive
while a plan holds them, which costs memory proportional to the writes
made during the longest in-flight plan. If `imbl` is declined, (b) ships
behind the same field types.

**Why (performance with simplicity).** (c) is what the 10k cell
measured: the "small linear term" from calls #2 is now 4.4 ms of
snapshot plus the drops inside `build_plan`, and it grew 3.1× for 2×
nodes. (a) gets the same O(1)-in-N result but adds a journal, overflow
handling, and per-thread state that must stay coherent with the fence.
That is exactly the cache-invalidation machinery calls #2 rejected the
first time. A type change at one struct boundary is the smallest diff
that removes the term, and it keeps value semantics, so every reader
stays correct by construction.

## 2. The fence epochs are sampled from the plan's own snapshot, and the lazy/eager split is retired

**Call.** `scope_epochs` is captured in the same snapshot as capacity,
and the submit-time sample reads the affected scopes out of it. The
fallback `loop`, `expected_versions_if_unchanged`, and the eager
whole-fleet sample are deleted. `schedulerSettings.lazy_version_sample`
becomes a no-op that warns at start:

```text
WARN schedulerSettings.lazy_version_sample is retired and has no effect:
     plans now sample the fence epochs from their own snapshot
```

**Alternatives.**
(a) Keep the lazy path and make the generation per-scope, so a plan
falls back only when one of *its* scopes was published.
(b) Keep everything and make the eager sample cheaper (it is O(N)
formatted keys and `BTreeMap` inserts).
(c) Keep the key meaningful by retaining the eager path behind it.

**Trade-off.** A config key stops doing anything. An operator who set it
to `false` to force the eager path gets the same guarantee without
asking, but the knob is gone. The fallback counter goes flat and is
retired a release later, so dashboards built on it go quiet.

**Why (simplicity, and the fence stays exact).** Sampling from the
snapshot is the eager guarantee itself (sample no later than the
capacity read), at O(affected) cost, with no re-plan under any churn
level. (a) still re-plans when a plan's own candidates move, needs a
second invariant about which scopes a plan "owns", and keeps two code
paths. (b) keeps the re-plan, which §5.1 estimates as the biggest term.
(c) keeps a second path alive only so a knob can select an inferior
version of the same guarantee.

## 3. Candidate truncation keeps today's semantics exactly, and only its cost changes

**Call.** The budgeted walk verifies as it walks and stops at the
budget, but it returns the same distribution as
`truncate_within_boundary_cell` today: whole ranks kept, the boundary
rank uniformly sampled among its verified members.

**Alternatives.**
(a) Adopt Nomad's or K8s's default: stop after a small, fleet-relative
number of feasible nodes and score only those (Nomad's max(2, log2 n),
K8s's adaptive `percentageOfNodesToScore`).
(b) Make the class-style constraints partition keys of the default
`fit` index, so the shard itself shrinks.

**Trade-off.** A selective constraint that is not a partition key still
costs O(budget ÷ selectivity), which approaches the shard size for very
rare attributes. The public doc says so and points at declaring an index.

**Why (apples-to-apples, and no silent packing change).** (a) changes
which nodes are considered at defaults, which is a packing-quality
decision, and the campaign forbids any fix that rides on a scoring
change. It deserves its own plan with a quality gate like
`score_sample_bin_pack_quality_stays_bounded`. (b) guesses which
attributes matter for everyone's fleet and multiplies shards, and an
index keyed on an attribute most fleets lack costs every node a
projection. Keeping semantics fixed means the phase can be judged on
cost alone.

## 4. Measurement ships first, as its own phase, and gates the fixes

**Call.** Phase 0 adds the `discarded_attempt` phase, a read-model lock
wait histogram, a node-list rebuild counter, the collector lines for
metrics that already exist, and a bench grid. No fix ships before it,
and Phase 0 stops the plan if the bench cannot reproduce the campaign's
direction.

**Alternatives.** Ship Phase 1 straight away, since §5.1-5.4 are already
located in code with estimated payoffs, and measure on the next
campaign.

**Trade-off.** A week or so before any throughput moves, and one extra
AWS session.

**Why (performance work needs a before).** The two largest terms in the
campaign are the ones nobody measured: 24 ms of unattributed glue and
~48 ms of estimated time outside planning. §5.1's explanation fits the
numbers but cannot be proven from them, and `apply`'s doubling has no
located cause. AGENTS.md requires before/after numbers for scheduler
changes, and the existing benches are single-threaded, so they cannot
show the fallback or the refcount contention at all. Recording the
discarded attempt as a phase also fixes a standing blind spot: the phase
table silently under-reports any re-planned eval.

## 5. The eval path's O(jobs) SQL scans are fixed with indexes, not read-model caches

**Call.** Phase 4 adds a migration with two indexes,
`job_update(to_job_config_id)` and `job_config(name, version DESC)`, and
relies on the `job_config(job_id, created_at)` index added alongside it
for the reconciler fix. Together they turn the per-eval previous-config
lookup, the plan apply's own-usage read (run on every fresh plan, under
the write connection), and RunJob's current-config read from table scans
into index seeks.

**Alternatives.** (a) Serve those reads from the scheduler read model
(a job → configs map, a config → previous-config map). (b) Leave them:
they were invisible on the single-job benches.

**Trade-off.** One more migration, so `schema_version` moves (it is
compared with `>=` by the raft upgrade gate, so mixed versions are
unaffected). A follower that installs a snapshot produced by an older
leader has no new indexes until its next restart, which costs speed only.
Each index costs a little on every insert into its table.

**Why (performance with simplicity).** The scans cost O(jobs), and the
benchmark grows jobs with nodes, so they grew super-linearly in fleet size:
locally the 5,000-node probe (4,166 jobs) spent 1.45 ms in `apply` before
and 0.67 ms after, and the whole eval cycle fell from 2.7 ms to 1.3 ms. A
cache would need its own invalidation on every config write for the same
result. An index is the database doing its job, and it fixes every caller
of the same shape at once, including ones nobody has profiled yet. Kept
separate from the reconciler's migration so either can land or revert on
its own.

## 6. Eval wall time gets its own phase histogram, including the blocking-pool wait

**Call.** A new `legion_eval_phase_seconds{kind, phase}` family splits the
handler into `queue_wait`, `scheduler_init`, `prepare`, `plan`,
`conflict_backoff`, `dispatch`, and `finish`, and
`legion_eval_queue_round_seconds{op}` times claim and settle. The plan
histogram gains `job_lookup`, `speculative_dispatch`, and `post_apply`,
and now observes rejected attempts.

**Alternatives.** (a) Add the eval phases as more labels on
`legion_scheduler_plan_phase_seconds`. (b) Keep deriving "outside
planning" as `eval_handle - total`, as Phase 0 planned.

**Trade-off.** Two more families to document and scrape. Rejected plans
now count in the plan histogram, so its rate is attempts rather than
successful plans, which shifts old dashboards slightly when conflicts are
common.

**Why (every millisecond attributable).** The 20k-node AWS run measured
`eval_handle` at 0.80 s against a 0.042 s plan, so 95% of an eval was a
derived remainder nobody could split. (a) would mix one-per-attempt and
one-per-eval series in one family and need a `kind` label the plan does
not have. (b) is exactly what failed. `queue_wait` exists because eval
handlers share the blocking pool with every heartbeat's `spawn_blocking`,
and a saturated pool otherwise reads as a slow handler.

## 7. Phase 0's runtime metrics were cut to what the fixes still needed

**Call.** Phase 0 shipped the `plan_fleet_scaling` bench and three
collector snapshots (lazy-sample fallback, `eval_handle` mean and p99,
index outcomes). It did not ship the `discarded_attempt` phase, the
read-model lock-wait histogram, the node-list rebuild counter, or the
`run-step.sh --profile` flag. Phases 1 and 2 then ran without a separate
"before" AWS re-run.

**Alternatives.** Build Phase 0 in full, re-run 5k and 10k on AWS on the
unmodified scheduler, then start Phase 1, as the plan says.

**Trade-off.** `apply`'s doubling (§ 5.5) has no direct lock-wait
evidence, so if it persists after Phase 3 its cause is still argued
rather than measured, and the lock histogram becomes the first thing to
build. The "before" for AWS is the 2026-10-09 campaign itself, which had
no `eval_handle` snapshot, so outside-planning time can be compared only
after the change.

**Why (the fixes removed what the metrics were for).** The
`discarded_attempt` phase existed to expose the lazy fallback's thrown-away
attempt, and Phase 1 deletes that loop, so the phase would always read
zero. The bench reproduced the campaign's direction locally (concurrent
per-plan total 5.9 → 12.7 ms from 5k to 10k, past the plan's 2× exit
criterion), and the rig's own Prometheus supplied the fallback rate after
the fact (27% and 55%), which settled the plan's biggest open question
without a re-run. The user asked for the fix and a retest, and an extra
AWS cycle before the code existed would have delayed that by hours for
numbers the campaign archive already held.

## 8. No escape hatch for Phases 1 and 2

**Call.** Both phases ship default-on with no `schedulerSettings` key to
turn them off. Rollback is the previous binary.

**Alternatives.** Per-lever kill switches, as
[plan-path-scale-levers](plan-path-scale-levers-plan_calls.md) shipped
for the lazy sample, group commit, and candidate lists.

**Trade-off.** If either change misbehaves in production, an operator
cannot turn it off with a config edit and has to downgrade.

**Why (simplicity).** The earlier switches guarded behavior changes: a
different commit shape, a different wire format, a different sample
timing. These phases change internal representations that produce the
same plans under the same fence. A switch would mean keeping two
snapshot implementations and two candidate walks alive and tested
indefinitely. The equivalence and fence suites are the guard instead.

## 9. `apply` gets no speculative fix

**Call.** The plan names four hypotheses for `apply` doubling (read-model
lock coupling, CPU saturation, coalescer queueing, index cell memmove)
and fixes none of them directly. Phase 3 re-measures after Phases 1-2
and fixes only what the data names.

**Alternatives.** Fix the cheap one now: index cells as `Vec<String>`
with O(cell) insert and remove could become a `BTreeSet`.

**Trade-off.** `apply` is the largest phase (43 ms at 10k), and this plan
may leave it untouched if the hypotheses are wrong.

**Why (performance work aimed by evidence).** The two likeliest causes
(H1, H2) are side effects of the O(N) work Phases 1-2 remove, so a
direct fix could be measuring the same win twice or fixing nothing. The
cell memmove is the only located O(N) term in `apply`, and at ~6 nodes
per eval it cannot account for 20 ms. The phase-3 findings history shows
what happens when a fix lands ahead of its measurement.

## 10. The local bench is a nodes × fill grid with a concurrent cell, not an extension of `plan_under_fill`

**Call.** A new `plan_fleet_scaling` group: `web_fleet` node mix, nodes
{1k, 2k, 5k, 10k} × fill {0, 5}, the api-shaped job, plus four-thread
cells with a background publisher at 5k and 10k.

**Alternatives.** Add a node axis to `plan_under_fill` (uniform nodes,
unconstrained job), or drive the macro matrix
(`tests/integration/scheduler_scale_benchmark.rs` with `LEGION_BENCH_CONCURRENCY`).

**Trade-off.** Another bench group to maintain, with a slow setup at
10k nodes and 50k allocations.

**Why (reproduce the rig, not an easier shape).** `plan_under_fill`'s
uniform, unconstrained fleet never exercises the class re-verification
that dominates §5.2, and it is single-threaded, so it cannot see §5.1 or
the refcount contention. The macro matrix has too much run-to-run spread
on one host to resolve millisecond deltas (recorded in
plan-path-scale-levers' Phase 4 acceptance). The grid also separates
node count from fill, which the campaign could not.

## 11. The available-node set is cached with its lookup and rebuilt off-lock, not kept in a persistent ordered map

**Call.** The read model caches `Arc<AvailableNodes>` (the id-sorted list
plus a by-id `HashMap`). Plans share it; any node mutation drops it
through `SchedulingReadModel::nodes_changed`, and the next plan rebuilds it
from a read-locked collection, unlocked sort and map build, and an install
that only lands if `nodes_epoch` has not moved. A plan with exclusions
shares the set unless an excluded node is in it.

**Alternatives.** (a) An `imbl::OrdMap<String, Arc<Node>>` of available
nodes maintained on every node write, so there is never a rebuild. (b)
Keep the rebuild under the write lock, as the old list cache did.

**Trade-off.** A node change still costs the next plan an O(fleet log
fleet) rebuild (about 2.5 ms at 20k nodes locally), and a churning fleet
pays it on every plan. `legion_scheduler_node_list_rebuilds_total` makes
that visible.

**Why (simplicity, measured by the rig).** On the rig, node mutations are
rare: heartbeats and checkpoints do not reproject nodes, so the rebuild
is amortized over thousands of plans. (a) would change every scan from a
slice to a tree walk and touch every `nodes: &[Arc<Node>]` site in
`build_plan` for no measured gain. (b) is what the old cache did, and
holding the write lock for a 20k-node sort stalls every heartbeat and
planner behind it.

## 12. The per-plan `node_lookup` and the node-list clone stay

> **Superseded by entry 11** (Phase 4, 2026-10-10). Kept as the record
> of why Phases 1-2 deferred it.

**Call.** `build_plan` still builds a `HashMap<&str, &Node>` over every
available node per plan, and `available_node_arcs` still clones its
cached `Vec`. Neither became the cached `AvailableNodes { sorted, by_id }`
the plan describes (§ 6.1, § 6.2). `allocations_by_job` also stays a `std`
map: a plan reads one job's entry from it, never clones the whole map,
so persisting it would buy nothing.

**Alternatives.** Cache `by_id` in the read model and hand plans the
`Arc` as planned.

**Trade-off.** A residual O(fleet) term remains: `build_plan` at fill 5
is 280 µs at 5,000 nodes and 384 µs at 10,000 locally, and the node
fetch is 7 → 15 µs. On the rig, where every phase ran roughly 10× the
laptop's cost, that is about 1 ms per plan at 10k.

**Why (smallest diff that removes the big terms).** The excluded-nodes
path filters the list per plan, so a cached `by_id` needs an exclusion
check threaded through every lookup site in `build_plan`, a wide change
for about a tenth of what Phases 1 and 2 removed. Phase 3's rig numbers
decide whether it is worth it.

## 13. Dispatch and batch follow-ups read the read model, not SQLite

**Call.** `dispatch_allocations` takes each placed node's `Arc` from the
read model (`Store::cached_node`, falling back to SQL if absent) and task
configs as the read model's shared list
(`Store::shared_task_configs_for_job_config`), and the batch follow-up
check reads the shared list too.

**Alternatives.** Keep the per-eval SQL reads and only switch them to
`prepare_cached`.

**Trade-off.** Dispatch reads a node as of the last projection rather than
as of a fresh SQL read. On the leader the projection runs at apply, before
dispatch, so the two agree; a node re-registering at a new address in
between already raced the old code the same way, and the heartbeat reply
heals any missed push.

**Why (performance).** Eight node reads with full JSON parses per eval
were the largest piece of `dispatch` (455 → 240 µs per eval in the 5k
probe), they competed with heartbeats for the read pool, and the read
model already held both values.

## 14. Phase 4 leaves the status-checker loop and the heartbeat path alone

**Call.** The reconciler queries that pinned the 20k leader's main thread
(`jobs_with_unrolled_latest_config`, `reconcile_missing_rollouts`), the
heartbeat handler, and the status-checker loop are fixed in a parallel
change, not here. `sync_missing_task_volume_claims` (one task-config query
per active job per tick, 9.5 s at 16.7k jobs before the `job_config` index
and 86 ms after) is recorded as a TODO rather than rewritten.

**Alternatives.** Fix everything the investigation found in one change.

**Trade-off.** The volume-claim pass stays O(jobs) per checker tick on the
scheduler leader until its TODO lands.

**Why (no conflicting edits).** Those functions were being changed in
parallel, and two changes to the same loop would have collided. The
index this phase relies on already takes the pass from seconds to tens of
milliseconds.

## 15. A lost node's replan retries in place, then hands off to a durable `job_reschedule` eval

**Call.** `Store::reconcile_job_config_allocations` retries a plan conflict
up to five times on a fresh snapshot (25-400 ms backoff). If every attempt
loses, it enqueues a `job_reschedule` eval for that job config and returns
`Ok`, so the eval's own conflict retries and requeue backstop converge the
job. The lost-node and drain passes call it after they have already
committed the removal.

**Alternatives.**
- Make removal and replan one replicated op. Rejected: JC-33/JC-35 split
  them on purpose, because the replan must read the removal's committed
  effects and writes the status lane.
- Have the requeued `node_lost_reschedule` eval remember which configs it
  emptied. Rejected: it needs new eval state, and the job-level eval
  already exists and is idempotent.
- Leave it to the node monitor's re-enqueue. That only re-runs while the
  lost node still has allocations, and the removal already emptied it, so
  it never fires. This was the bug.

**Trade-off.** A reconcile can now sleep up to about 0.4 s on its thread
before handing off, and a job that keeps losing races converges one eval
later, not inline.

**Why (correctness, no lost workloads).** The 10k storm drill on
2026-10-10 left 22 of 6,343 allocations unplaced for good. Two reschedule
evals for one job raced on its epoch. The loser was requeued, found the
node already emptied, and returned success without placing anything. A job
silently running below its count is the worst failure a scheduler can
have. Covered by `lost_node_replacement_survives_plan_conflicts`, in both
the in-place and the hand-off case.

## 16. The heartbeat path keeps reading SQLite, with cached statements, not the read model

**Call.** `allocs_for_node`, `stop_notices_for_node`, and
`volumes_to_destroy_for_node` stay SQL reads and use `prepare_cached`. The
heartbeat does not serve a node's desired set from the scheduler's
`allocations_by_node` read model.

**Alternatives.** Serve the desired set from the read model, an O(1) map
lookup with no SQLite at all. Or skip the rebuild when the worker reports a
desired-state version that matches the node's current one, which is a wire
change.

**Trade-off.** Every beat still runs about three indexed queries. At 20k
nodes on a fixed 2 s cadence that is about 30k queries per second. Adaptive
cadence, now the default, cuts it to about 3k.

**Why (stability).** The heartbeat reply is what a worker reconciles
against, so a drifted read-model entry would make a node stop a healthy
allocation. The read model is repaired by a round-robin scrub, which is
fine for placement but not for something that can kill a workload. The
version-skip is the right long-term shape and is a TODO, since it needs a
proto field and worker support.
