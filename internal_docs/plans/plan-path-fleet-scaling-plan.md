# Plan: Per-Eval Scheduler Cost That Does Not Grow With the Fleet

> **Judgement calls:** [`../calls/plan-path-fleet-scaling-plan_calls.md`](../calls/plan-path-fleet-scaling-plan_calls.md)

**Status: IN EXECUTION (2026-10-10).** Phases 1 and 2 are built and
pass the full suite. Phase 3's AWS retest ran the same day (results
below): 10k throughput doubled, 5k rose 46%. Phase 4 (§ 13, built
locally, AWS re-run pending) answers the 20k cell: it removes the
per-eval node lookup and the eval path's O(jobs) SQL scans, and splits
the eval's wall time into named phases.
Phase 0 shipped partly: the `plan_fleet_scaling` bench and the
collector's fallback, `eval_handle`, and index-outcome snapshots exist,
while the `discarded_attempt` phase, the read-model lock-wait histogram,
the node-list rebuild counter, and the leader `perf` option did not
(calls entry 7). Also deferred at the time: caching the per-plan `node_lookup` and
the available-node `Vec` clone (calls entry 12, done in Phase 4), and, as in the
non-goals, candidate construction for tasks that cannot truncate.

**Local result** (`plan_fleet_scaling`, M-series laptop, fill 5, per
plan):

| Cell | Before | After |
| --- | --- | --- |
| concurrent, 5,000 nodes | 5.9 ms, 86% re-planned | 3.8 ms, 0% |
| concurrent, 10,000 nodes | 12.7 ms, 96% re-planned | 3.7 ms, 0% |
| single-threaded `build_plan`, 10,000 nodes | 2,592 µs | 384 µs |
| single-threaded `capacity_snapshot`, 10,000 nodes | 244 µs | 0 µs |

Per-plan cost under concurrency is now flat from 5k to 10k nodes. The
remainder is the local commit (`apply`), which the plan never targeted.

**AWS result (Phase 3, 2026-10-09)**, same rig and defaults as the
campaign that motivated the plan (3× `m6id.xlarge`, `evalThreads` 4),
binary `e77f3e8-dirty-3551b12a5b8e`:

| `web_fleet`, 5 allocs/node | Before | After |
| --- | --- | --- |
| 1,000 nodes | 764/s, p99 4.1 s | 733/s, p99 4.4 s |
| 2,000 nodes | 663/s, p99 8.3 s | 669/s, p99 8.4 s |
| 5,000 nodes | 504/s, p99 39 s | 736/s, p99 30 s |
| 10,000 nodes | 163/s, p99 257 s | 322/s, p99 112 s |
| storm, 1,250 of 10,000 nodes killed | 139 s to recover | 114 s to recover |

| `dense_fleet` at 5,000 nodes | Before | After |
| --- | --- | --- |
| 5 allocs/node | 1,983/s | 2,401/s |
| 10 allocs/node | 1,333/s | 2,049/s |
| 20 allocs/node | 1,400/s | 1,801/s |
| 40 allocs/node | 719/s | 1,049/s |

1k and 2k are unchanged, consistent with those sizes being bound by the
commit path rather than planning. The 10k cell lost 4 nodes of one
simulator slice to heartbeat expiry and replaced their allocations
(20 extra placements counted). Leader heartbeat freshness dipped as low
on the old binary (minimum 0.72 against 0.70), and leader CPU fell from
2.9 to 2.45 cores, so this is the pre-existing 10k heartbeat pressure,
not a regression. Still open: the 10k curve still bends (322/s against
736/s at 5k), and § 5.5's `apply` question now has `eval_handle` data in
the archive to answer it.

Builds on, and does not re-propose, the shipped work in
[`plan-path-scale-levers-plan.md`](plan-path-scale-levers-plan.md) (lazy
version sample, group commit, cheap snapshots),
[`scheduler-plan-snapshot-plan.md`](scheduler-plan-snapshot-plan.md)
(Arc'd nodes and pools), [`scheduling-index-plan.md`](scheduling-index-plan.md)
(the `fit` index), and the 2026-09-02 fill-slope pass in
[`scheduler-performance-plan.md`](scheduler-performance-plan.md) (per-job
allocation index). Those plans removed the costs that grew with cluster
*fill*. This one removes the ones that still grow with *node count*.

## 1. Problem

The 2026-10-09 AWS campaign (binary at `main` `e77f3e8`) ran `web_fleet`
at 5 allocations per node on 3× `m6id.xlarge` managers (4 vCPU, raft
lanes, `evalThreads` 4 = the shipped default), simulated workers, native
gRPC submitter, whole workload in one burst.

| Nodes | Throughput | submit→placed p50 / p99 |
| --- | --- | --- |
| 1,000 | 764 alloc/s | 3.2 s / 4.1 s |
| 2,000 | 663 alloc/s | 7.3 s / 8.3 s |
| 5,000 | 504 alloc/s | 33 s / 39 s |
| 10,000 | 163 alloc/s | 140 s / 257 s |

Nomad 2.0.5 measured 122/s at 1k and 110/s at 2k. Kubernetes 1.36 (API
QPS raised) measured 117, 78 and 53 at 1k, 2k and 5k. Legion leads every
cell, but its curve bends hard between 5k and 10k: doubling the fleet
cost 68% of throughput.

Per-eval plan cost from `legion_scheduler_plan_phase_seconds`
(sum-rate over count-rate across the cell window):

| Phase | 5k nodes | 10k nodes | Growth for 2× nodes |
| --- | --- | --- | --- |
| total | 36 ms | 99 ms | 2.75× |
| apply | 21 ms | 43 ms | 2.0× |
| build_plan | 8.7 ms | 23 ms | 2.6× |
| capacity_snapshot | 1.4 ms | 4.4 ms | 3.1× |
| version_sample | 0.8 ms | 3.8 ms | 4.5× |
| task_fetch | 0.2 ms | 0.4 ms | 2× |
| available_nodes | 0.15 ms | 0.4 ms | 2.7× |
| *total minus the phases* | *3.75 ms* | *24 ms* | *6.4×* |

The last row is derived, and it is the largest single growth term in the
table. Raft pipeline wait stayed at 1-3 ms and store write-lock wait p99
at 0.5 ms at both sizes, so this is not commit contention. The leader
averaged 1.35 of 4 cores at 5k and 2.9 (max 3.7) at 10k, while followers
sat near 0.2. At 10k the leader is close to CPU-bound.

Three things limit what this data can prove, and the plan carries all
three forward:

- **Nodes and allocations both doubled.** Allocations per node were held
  at 5, so the cluster went from 25k to 50k allocations along with the
  node count. The phase table alone cannot separate "grows with nodes"
  from "grows with fill". The code reading in §5 attributes each term,
  and Phase 0's local bench varies the two axes independently to confirm
  it.
- **The phase histogram covers planning only.** `legion_eval_handle_seconds`
  (end-to-end eval wall time) exists in the binary but the AWS collector
  does not scrape it. Backing it out of throughput (4 threads, ~6
  allocations per `web_fleet` eval) gives roughly 48 ms per eval at 5k
  and 147 ms at 10k, so about 12 ms and 48 ms per eval happen outside the
  planning phases. That is an estimate from Little's law, not a
  measurement.
- **The fallback rate of the lazy version sample was not collected.**
  `legion_scheduler_lazy_sample_fallback_total` exists but is not
  scraped, and §5.1 shows it is the most likely explanation for the
  6.4× row above.

What the user has to do today instead: nothing they can do at defaults.
The knobs that would help (`score_sample`, a declared `class` index, more
`evalThreads`) are opt-in, and the benchmark forbids them by design.

## 2. Prior art

**Nomad.** The state store is
[go-memdb](https://github.com/hashicorp/go-memdb), a set of immutable
radix trees. `StateStore.Snapshot()` is O(1): a scheduler worker plans
against a frozen tree root, and writers path-copy. The
[plan applier](https://github.com/hashicorp/nomad/blob/main/nomad/plan_apply.go)
re-checks only the nodes a plan touches against the latest snapshot.
Nomad does still walk the ready node list per eval
([`readyNodesInDCsAndPool`](https://github.com/hashicorp/nomad/blob/main/scheduler/util.go))
and shuffles it, but feasibility checking is a lazy iterator chain that
stops once the
[`LimitIterator`](https://github.com/hashicorp/nomad/blob/main/scheduler/select.go)
has enough feasible nodes (max(2, log2 n) for services).

**Kubernetes.** The scheduler's
[cache](https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/backend/cache/cache.go)
keeps node infos in a list ordered by a generation number, and
`UpdateSnapshot` copies only the nodes whose generation moved since the
previous cycle. A cycle costs O(changes), not O(nodes). Filtering then
stops after
[`percentageOfNodesToScore`](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduler-perf-tuning/)
feasible nodes (adaptive, 50% at 100 nodes down to 5% at 5,000, floor
100), starting from a rotating offset so the same nodes are not always
checked first.

**Consul.** Same go-memdb model as Nomad. Catalog reads at 10k+ nodes run
against immutable snapshots, and blocking queries watch radix-tree nodes
instead of rescanning.

**What we take.**

- An O(1) snapshot of the scheduler's state (Nomad/Consul's immutable
  trees) or an O(changes) one (K8s's generation list). Both make the
  per-plan copy independent of fleet size. §6.1 picks between them.
- Stopping candidate construction once there are enough verified
  candidates (Nomad's iterator chain, K8s's feasible-node cutoff).
  Legion already truncates to a budget, but only *after* materializing
  and verifying the whole shard.

**What we reject.**

- Changing *which* nodes are scored by default. K8s and Nomad both
  sample by default, and Legion keeps exact best-first candidate
  selection at defaults (sampling is the opt-in `score_sample`). This
  plan changes the cost of producing the same candidate set, not the
  set. A default-sampling change is a product decision with packing
  consequences and belongs in its own plan.

## 3. Jobs to be done

**Cluster operator**
- When my fleet grows from 5,000 to 10,000 machines, I want a deploy of
  the same size to take about as long as it did before, so growing the
  cluster does not quietly make every rollout slower.
- When I read the scheduler's metrics after a slow burst, I want every
  millisecond of an eval accounted for, so I can tell planning from
  commit from everything else without guessing.

**Job author / app developer**
- When I ship a small service into a very large cluster, I want it placed
  in seconds, so the size of the cluster I happen to deploy into is not
  my problem.

**On-call responder**
- When a rack dies in a 10,000-node cluster and hundreds of services need
  new homes, I want replacements placed at the same per-service speed as
  in a small cluster, so recovery time depends on how much broke, not on
  how big the fleet is.

**Legion maintainer (the public benchmark)**
- When I publish the scaling curve against Nomad and Kubernetes, I want
  it to stay flat at defaults out to 10k nodes, so the claim holds
  without any tuning a reader could call unfair.

*Not separately served:* the autonomous agent persona. An agent
submitting work sees exactly the job author's latency, and this plan
adds no surface for it to drive. The security admin persona is out of
scope, since nothing here touches a boundary.

### User scenarios

1. **The burst.** An operator submits 8,000 small services at once into a
   10,000-node cluster (the campaign shape). Today the last one places
   after ~4 minutes. With this plan's phases shipped, the per-eval cost
   at 10k should sit near the 5k cost, so the drain time scales with the
   number of services and not with nodes × services.
2. **The storm.** A 1,250-node slice of a 10,000-node fleet is lost.
   Every `node_lost_reschedule` eval runs the same plan path, so the
   same per-eval fleet tax applies to every replacement.
3. **The quiet deploy.** One job into an idle 10,000-node cluster. No
   concurrent publishes, so the lazy sample never falls back, but the
   snapshot, `node_lookup`, and shard walk still cost O(nodes) on every
   eval. Latency here is milliseconds either way, so this scenario is a
   regression guard, not a target.

## 4. Goals and non-goals

**Goals**

- At default settings, per-eval planning cost for a task whose
  candidates may truncate (the common case: no `count.per`, no
  `distinct`, no volumes, single-task group) does not grow with node
  count.
- Every eval's wall time is attributable: the plan phases plus a named
  "outside planning" remainder, both scraped by the AWS collector.
- The oversubscription fence keeps its exact guarantee: the epoch sample
  is never later than the capacity read the plan was built against.
- Each phase ships with same-host before/after numbers (AGENTS.md's
  scheduler rule) from a bench that reproduces the 5k→10k growth.

**The apples-to-apples constraint.** No fix may depend on non-default
tuning or a benchmark-only knob. Not `score_sample`, not
`nodes_considered`, not a declared `class` index, not raised
`evalThreads`, not a changed `index_min_nodes`. A fix lands as the
default behavior or it does not count for this plan.

**Non-goals**

- Non-truncatable tasks (`count.per`, `distinct`, volume-pinned,
  multi-task groups). They still scan the fleet in `scan_base_eligible`
  and the `group_candidates` pass (`src/manager/scheduler.rs:2016`).
  Follow-up item, named in TODO.md.
- Default scoring or sampling changes (see §2 "What we reject").
- The `evalThreads` default ([`eval-thread-default-plan.md`](eval-thread-default-plan.md)
  owns it).
- Heartbeat and liveness cost. It grows with nodes too and competes for
  the leader's CPU, but [`liveness-off-the-raft-path-plan.md`](liveness-off-the-raft-path-plan.md)
  and the heartbeat plans own it. Phase 0's profile says whether it
  matters here.
- The raft commit path. The campaign shows no commit contention.
- `legion scheduler diagnose`
  ([`scheduler-diagnosis-tooling-plan.md`](scheduler-diagnosis-tooling-plan.md)).
  Phase 0 adds metrics that tool would read, not the tool.

## 5. What exists today: each source of fleet-size growth

Every term below is per eval. Ranked by expected payoff at 10k nodes.

### 5.1 The lazy version sample falls back, and the discarded attempt is invisible (est. ~10-24 ms + 1-4 ms)

`plan_and_apply_internal` plans lazily by default: it reads capacity and
the read-model `generation` under one guard, plans, then samples epochs
for only the affected scopes if `generation` has not moved
(`Store::expected_versions_if_unchanged`, `src/storage/store.rs:3749-3772`).
If it moved, the scheduler sets `eager_sample = true`
(`src/manager/scheduler.rs:1602`) and **re-runs the whole attempt**:
`available_nodes`, task fetch, capacity snapshot, and `build_plan`, plus
the eager O(fleet) sample (`allocation_version_vector_for_candidates`,
`src/storage/store.rs:3701-3725`, one formatted `node:<id>` key and one
`BTreeMap` insert per available node).

The generation is global. Every allocation publish bumps it: the full
scope refresh (`store.rs:3022`), node removal (`store.rs:2901`), and the
delta projection that every raft changeset touching an allocation row
goes through (`store.rs:3264`). With 4 eval threads applying plans (and
any other allocation write landing in between), the chance that
*something* is published during one plan's window rises with both the
publish rate and the plan duration. Plan duration grows with the fleet.
Which other writers publish during a burst is not measured, and Phase 0's
fallback counter bounds it. The prior plan accepted
this cost on purpose ("under constant churn every plan pays one wasted
`build_plan`", [calls #1](../calls/plan-path-scale-levers-plan_calls.md)).

Why it explains the 6.4× row: each attempt *overwrites* the phase fields
(`timings.available_nodes_us` at `scheduler.rs:1425`, and likewise
`task_fetch_us`, `capacity_snapshot_us`, `build_plan_us`), and
`total_us` covers both attempts. So a discarded first attempt lands in
"total minus phases" and nowhere else. If the fallback rate is *f*, glue
≈ *f* × (build + snapshot + nodes + tasks) and `version_sample` ≈ *f* ×
eager cost. At 10k, glue is 24 ms against a ~28 ms attempt, which
implies *f* ≈ 0.85. At 5k, 3.75 ms against ~10.5 ms implies *f* ≈ 0.35.
**This is consistent, not proven.** The eager sample's per-node cost on
the rig is unknown (the 2026-07 local measurement was ~1.2 ms per 1k
nodes, which would put *f* nearer 0.3 at 10k and leave part of the glue
unexplained). Phase 0 scrapes the fallback counter and records the
discarded attempt as its own phase, which settles it.

**Measured after the fact (2026-10-09).** The fallback counter was never
archived by `collect.sh`, but the rig's Prometheus still held it.
`increase(legion_scheduler_lazy_sample_fallback_total)` against
`increase(legion_scheduler_plan_phase_seconds_count{phase="total"})` over
each cell's window:

| Cell | Plans | Fell back | Rate |
| --- | --- | --- | --- |
| `web_fleet` 5,000 nodes | 4,226 | 1,156 | **27%** |
| `web_fleet` 10,000 nodes | 8,428 | 4,600 | **55%** |

So the fallback rate doubles with the fleet, which confirms the
mechanism. The rates sit below the 0.35/0.85 inferred from the glue,
which means the discarded attempt does not explain the whole 24 ms of
glue at 10k: roughly half of it is still unaccounted, and Phase 0's
per-phase accounting is still needed to find the rest. The windows are
approximate (taken from the cell log timestamps, 6 minutes each), so
treat the rates as ±a few points.

Grows with: nodes (eager sample and re-plan cost), and with the publish
rate (more threads, more status writes). Not with fill per se.

### 5.2 `build_plan` materializes and re-verifies the whole index shard per task (est. ~10-15 ms)

The default `fit` index partitions by `pool` only
(`default_fit_spec`, `src/scheduling_index.rs:153-169`), and simulated
fleets put every node in one pool. So for every task the shard is the
whole fleet. Per task, per eval:

- `IndexSet::query_with_order` is called with `budget: None`
  (`scheduler.rs:2400`), ranks the shard's cells, then **clones every
  node id in the shard** into a `Vec<(String, u32)>`
  (`src/scheduling_index.rs:789`), all under the read-model *read* lock
  (`Store::scheduling_index_query`, `store.rs:3639`).
- The scheduler looks up every returned id in `node_lookup` and
  re-verifies constraints, permit, and volume drivers on every one
  (`scheduler.rs:2436-2450`).
- Only then does it truncate to `candidate_budget`
  (`scheduler.rs:2527-2529`): max(8 × needed, 64, `plan_apply_batch_max`
  × `plan_candidates.count`) = 128 at defaults.

So a `web_fleet` api job (two tasks, `class = "web"`) clones, hashes, and
verifies ~20k ids at 10k nodes to keep 256. The `class` constraint is not
a partition key of the default index, so it is checked per node rather
than selected by shard.

On top of that, once per plan:

- `node_lookup` is a fresh `HashMap<&str, &Node>` over every available
  node (`scheduler.rs:1899-1902`).
- The snapshot's three O(N) containers (the node `Vec` and two
  `HashMap`s, §5.3) are dropped at the end of `build_plan`, which is
  O(N) `String` frees and atomic decrements inside the `build_plan`
  timer.

Grows with: nodes. Fill changes how ids spread across cells, not how
many there are.

### 5.3 The capacity snapshot clones two fleet-sized maps (est. ~4 ms)

`Store::plan_capacity_snapshot` (`store.rs:3545-3557`) clones
`allocations_by_node` and `node_pools` under the read guard. Since the
snapshot plan these are `HashMap<String, Arc<_>>`, so a clone is one
`String` allocation plus one atomic increment per node, per map. The
prior plan recorded this as an accepted linear term ([calls
#2](../calls/plan-path-scale-levers-plan_calls.md): "truly enormous
fleets still pay a small linear term").

The 3.1× growth for 2× nodes is superlinear. Two plausible reasons, both
unmeasured: four planners incrementing and decrementing the *same* 20k
refcounts bounce those cache lines between cores, and at 10k the working
set (two maps plus their keys) stops fitting in the per-core caches.
Phase 0's concurrent bench should show which.

Grows with: nodes. The per-job index removed the fill term on
2026-09-02.

### 5.4 The available-node list is cloned per plan (est. ~0.4 ms)

`Store::available_node_arcs` (`store.rs:3508-3533`) caches the sorted
list as `Arc<Vec<Arc<Node>>>` but returns `Vec::clone(cache)`
(`store.rs:3512`), so every plan pays N refcount increments, and N
decrements when it drops them. Same cache-line contention argument as
§5.3. Every node mutation also invalidates the cache, and the next plan
rebuilds it with an O(N log N) sort under the read-model *write* lock.
The invalidation rate under load is unmeasured.

Grows with: nodes.

### 5.5 `apply` doubled, and the code does not say why (unknown, up to ~20 ms)

`apply` spans the coalescer submit, the raft round, the leader's
changeset projection, and the status-lane bookkeeping
(`scheduler.rs:1639-1700`, `store.rs:4350-4405`). Nothing in it walks
the fleet. Candidate explanations, in the order we believe them:

- **H1, read-model lock coupling.** Every publish takes the read-model
  write lock. Planners hold the read lock for O(N) work: the snapshot
  clone (§5.3), the node-list clone (§5.4), and the shard id clone
  (§5.2). Linux's `std::sync::RwLock` prefers writers, so a waiting
  writer blocks new readers too. O(N) read holds therefore stretch every
  publish, and every publish then stalls every planner. No metric
  measures this lock today.
- **H2, CPU saturation.** At 2.9 of 4 cores average and 3.7 peak,
  everything on the leader runs slower, including the apply path.
- **H3, coalescer queueing.** `apply` includes waiting for the batch in
  flight. `legion_store_coalesce_wait_seconds` is already scraped, so
  the campaign archive can confirm or refute this without a re-run.
- **H4, index cell maintenance.** Each reprojected node is removed from
  and inserted into a sorted `Vec<String>` cell
  (`scheduling_index.rs:489`, `:503`). In a homogeneous fleet one cell
  can hold thousands of ids, so each move is an O(cell) memmove. Likely
  small (~6 nodes per eval), but it does grow with nodes.

H1 and H2 are side effects of §5.1-5.4, so fixing those may fix `apply`
without touching it. Phase 3 checks before anything is built here.

### 5.6 Per-eval time outside planning (estimated ~12 → ~48 ms)

The handler (`Eval::handle_job_submit`, `src/structs/eval.rs:131`) also
reads the previous config, runs `dispatch_allocations`
(`src/manager/dispatcher.rs:57`), and schedules follow-ups. The eval
loop then settles the batch in one `complete_evals_and_claim_next` round
(`src/manager/daemon.rs:2127`). None of this obviously walks the fleet.
The estimated 4× growth may be the same CPU and lock pressure as H1/H2.
Phase 0 replaces the estimate with `eval_handle_seconds`.

### 5.7 Smaller terms, recorded and left alone

- `read_model_node_counts` (`store.rs:3480`) walks every node for a
  gauge, on the metrics tick, not per eval.
- `task_fetch` doubled (0.2 → 0.4 ms). The phase reads only this job's
  config, so the growth is most likely the read pool and CPU pressure
  noted in TODO.md's read-connection entry. Too small to chase.

### Payoff summary

| # | Source | Evidence | Grows with | Est. saved at 10k |
| --- | --- | --- | --- | --- |
| 5.1 | Lazy-sample fallback re-plans | `scheduler.rs:1602`, `store.rs:3756`, `:3264` | nodes × publish rate | 10-24 ms glue + 1-4 ms sample |
| 5.2 | Whole-shard id clone + verify per task, `node_lookup` per plan | `scheduling_index.rs:789`, `scheduler.rs:2436`, `:1899` | nodes | 10-15 ms |
| 5.3 | Snapshot map clones | `store.rs:3548-3549` | nodes | ~4 ms |
| 5.4 | Node list clone | `store.rs:3512` | nodes | ~0.4 ms |
| 5.5 | `apply` doubling | not located (H1-H4) | unknown | 0-20 ms |
| 5.6 | Outside-planning time | not measured | unknown | unknown |

Fixing 5.1-5.4 targets roughly 30-45 ms of the 99 ms per eval at 10k,
which would put planning near the 5k figure. If H1 holds, `apply` falls
with them. These are projections, and Phase 0 exists so that each later
phase can be judged against measured numbers instead.

## 6. Proposed design

Nothing in this plan changes a jobspec, CLI verb, RPC, or on-disk format.
The user-visible surface is throughput, latency, a few metrics, and one
retired config key. The surface shown below is what an operator reads.

### 6.1 One O(1) plan snapshot, with the fence epochs inside it (fixes 5.1, 5.3, 5.4)

Make the scheduling read model's plan-facing maps persistent, so taking
a snapshot is a pointer copy and a write path-copies only what it
touches:

- `allocations_by_node`, `node_pools`, `scope_epochs`, and
  `allocations_by_job` become persistent hash maps (HAMT). A snapshot
  is a handful of root-pointer clones under the read guard, O(1) in
  fleet size. Writers keep their current call sites. A persistent map's
  `insert` and `get_mut` copy only the O(log₃₂ N) path to the entry.
- `available_nodes_cache` returns its `Arc` instead of cloning the `Vec`.
  The cached value becomes `AvailableNodes { sorted: Vec<Arc<Node>>,
  by_id: HashMap<String, usize> }`, built only when the node set
  changes. `build_plan` borrows it, and `by_id` replaces the per-plan
  `node_lookup` (§6.2 uses it).

**The fence sample comes from the snapshot.** Because `scope_epochs` is
captured under the same guard as `allocations_by_node` and `node_pools`,
the epochs in the snapshot are the epochs of exactly the state the plan
was built against. Reading the affected scopes out of the snapshot at
submit time is a sample taken *at* the capacity read, which is the
eager path's guarantee, at O(affected) cost. The mirror invariant
(mirror epoch ≤ the epoch of the state the cached allocations came from,
`SchedulingReadModel::scope_epochs`) holds for any published state, so
the CAS stays conservative exactly as today.

That makes the lazy/eager split unnecessary. The `loop` in
`plan_and_apply_internal`, `expected_versions_if_unchanged`, and the
eager whole-fleet sample are deleted. A plan never re-runs because
something unrelated published, so `legion_scheduler_lazy_sample_fallback_total`
stops moving and is retired after one release. The generation counter
stays, since the snapshot still carries it for diagnostics.

What an operator sees:

```text
# before (10k nodes, burst)
legion_scheduler_plan_phase_seconds{phase="version_sample"}  mean 3.8ms
legion_scheduler_plan_phase_seconds{phase="capacity_snapshot"} mean 4.4ms
legion_scheduler_lazy_sample_fallback_total                    rising

# after
legion_scheduler_plan_phase_seconds{phase="version_sample"}  mean <0.05ms
legion_scheduler_plan_phase_seconds{phase="capacity_snapshot"} mean <0.05ms
legion_scheduler_plan_phase_seconds{phase="discarded_attempt"} count 0
```

`schedulerSettings.lazy_version_sample` loses its meaning: there is one
sample, and it has the eager guarantee. The key stays accepted, does
nothing, and logs one warning at manager start:

```text
WARN schedulerSettings.lazy_version_sample is retired and has no effect:
     plans now sample the fence epochs from their own snapshot
```

**The dependency.** The persistent map is
[`imbl`](https://crates.io/crates/imbl), the maintained fork of `im`.
That is a new dependency and needs agreement before Phase 1 starts (see
§11). If it is declined, the fallback is a dependency-free sharded
copy-on-write map (256 `Arc<HashMap>` shards, O(256) snapshot, writes
copy one shard of ~40 entries at 10k). It has the same interface at the
`SchedulingReadModel` field boundary, so the choice stays local. The
judgement calls doc records why a K8s-style per-worker incremental
snapshot lost.

### 6.2 Budgeted, verify-as-you-walk index candidates (fixes 5.2)

Today the index returns the whole shard and the scheduler verifies all
of it, then truncates. Move verification *into* the walk and stop when
the budget is met:

- `IndexSet::walk_verified(bindings, request, budget, verify)` ranks the
  shard's cells exactly as `query_with_order` does today. It then walks
  cells best-first, calling `verify(&str) -> Option<&Node>` (a lookup in
  `AvailableNodes::by_id` plus the existing constraint, permit, and
  driver predicate) without cloning ids.
- A cell whose raw size fits in the remaining budget is verified in full
  and kept in order. At the first cell that does not fit, the walk draws
  members in uniformly random order and verifies each until the
  remaining budget is filled or the cell is exhausted. If it runs out,
  it moves to the next cell.
- The output is the same distribution `truncate_within_boundary_cell`
  produces today. Every rank that fits after verification is kept whole,
  and the boundary rank is a uniform sample of its *verified* members.
  The one-slice concentration fix from
  [`aws-phase5-findings.md`](../aws-phase5-findings.md) is preserved.

Cost per task drops from O(shard) to O(budget ÷ selectivity), where
selectivity is the fraction of the shard that passes verification. For
`web_fleet` that is 128 ÷ 0.83 ≈ 155 checks for an api task and 128 ÷
0.17 ≈ 750 for a cron task, independent of fleet size. The walk runs
under the read-model read lock, as the query does today, but holds it
for O(budget) instead of O(shard). That is also the H1 fix.

Unchanged: the untruncated path (`allow_truncation == false`), shadow
mode, the empty-result scan fallback, and the refill-on-exhaustion scan
(`scheduler.rs:2725`). They stay O(N) and stay rare or opt-in. A
constraint that is very selective and not a partition key of any index
still costs O(budget ÷ selectivity), which can approach the shard size.
`docs/scheduling-indexes.md` gains one sentence saying so, with the
existing remedy: declare an index partitioned by that attribute.

### 6.3 Account for every millisecond of an eval (Phase 0, enables the rest)

- A new plan phase, `discarded_attempt`, records the wall time of any
  attempt the plan threw away. Today that is the lazy fallback. After
  §6.1 it should always be zero, which makes it the regression alarm.
  `total` then equals the sum of the phases plus small glue, which we
  document.
- A new histogram, `legion_store_read_model_lock_wait_seconds{mode}`
  (`mode` = `read` | `write`), times acquisition of the scheduler read
  model's `RwLock` in `read_model_reader` / `read_model_writer`
  (`store.rs:2598-2608`). This is the H1 test.
- A new counter, `legion_scheduler_node_list_rebuilds_total`, increments
  when `available_node_arcs` rebuilds its cache. It says whether the
  O(N log N) rebuild is rare in practice.

The AWS collector (`benchmarks/aws/scripts/collect.sh`) adds:

```text
snapshot eval_handle_mean  'sum by (kind) (rate(legion_eval_handle_seconds_sum[1m])) / sum by (kind) (rate(legion_eval_handle_seconds_count[1m]))'
snapshot eval_handle_p99   'histogram_quantile(0.99, sum by (kind, le) (rate(legion_eval_handle_seconds_bucket[1m])))'
snapshot lazy_sample_fallback_rate 'rate(legion_scheduler_lazy_sample_fallback_total[1m])'
snapshot index_queries     'sum by (index, outcome) (rate(legion_scheduler_index_queries_total[1m]))'
snapshot read_model_lock_wait_mean '...{mode} sum/count, same shape'
snapshot node_list_rebuilds 'rate(legion_scheduler_node_list_rebuilds_total[1m])'
```

`benchmarks/aws/analysis/cell_summary.py` derives two numbers per cell:
*outside planning* = `eval_handle` mean − plan `total` mean, and *glue*
= `total` − Σ phases. An optional `--profile` flag on `run-step.sh`
captures 30 s of `perf record -g` on the leader mid-cell, so the CPU
split between planning, apply, and heartbeats is read off a flame graph
instead of argued about.

### 6.4 A local bench that reproduces the growth (Phase 0)

A new Criterion group, `plan_fleet_scaling`, in
`benchmarks/micro/scheduler_bench.rs`:

- **Fleet:** a `web_fleet` node mix (83% `class = web`, 17%
  `class = util`, one pool), so the default `fit` index and the
  constraint re-verification run exactly as on the rig.
- **Grid:** nodes {1k, 2k, 5k, 10k} × fill {0, 5 allocations per node}.
  Rows vary nodes at fixed fill. Columns vary fill at fixed nodes. That
  separates the two axes the campaign confounded.
- **Planned job:** the `web_fleet` api shape (two single-task groups,
  4 placements each, `cpu: 500, memory: 1024`, `class = "web"`).
- **Cells:** `plan_only` and `plan_and_apply`, single-threaded, with the
  `print_phase_means` output `plan_under_fill` already prints.
- **Concurrent cells** at 5k and 10k, fill 5: four planner threads (the
  4-core default) running `plan_and_apply` on distinct jobs while a
  background thread places and removes a filler allocation at a fixed
  rate, so the read model publishes the way unrelated applies make it
  publish on the rig. Reports phase means, `discarded_attempt` count, and fallback
  count. This is the only shape that can reproduce §5.1 and the
  refcount contention in §5.3, since neither appears single-threaded.

Phase 0's exit criterion is that the bench reproduces the *direction*
of the campaign: per-plan `total` grows by more than 2× from 5k to 10k
nodes at fixed fill in the concurrent cell. If it does not, the plan
stops and the gap between bench and rig becomes the next question.

## 7. Proto, struct, config, and persistence changes

- **Proto / wire / on-disk:** none. No migration.
- **Job config:** no new fields, so nothing for `configs/jobs/maximal.ncl`.
- **Manager config:** `schedulerSettings.lazy_version_sample` is retired
  to a no-op with a startup warning (Phase 1). Removal after one release.
- **Metrics (`docs/metrics.md`):** new phase label `discarded_attempt`,
  new `legion_store_read_model_lock_wait_seconds{mode}`, new
  `legion_scheduler_node_list_rebuilds_total`. Retire
  `legion_scheduler_lazy_sample_fallback_total` one release after
  Phase 1.
- **Structs:** `SchedulingReadModel` field types (persistent maps),
  `available_nodes_cache` → `Option<Arc<AvailableNodes>>`,
  `CapacitySnapshot` gains `scope_epochs`, `IndexSet::walk_verified`.
- **Dependencies:** `imbl` (Phase 1, pending agreement), with
  `Cargo.lock` in the same commit.

## 8. Implementation phases

Each phase ships alone and is worth shipping alone.

**Phase 0: measure.** §6.3 metrics and collector, §6.4 bench, and one
AWS re-run of the 5k and 10k `web_fleet` cells on the unmodified
scheduler. Value on its own: it settles §5.1's fallback rate, gives
`apply` and outside-planning time real numbers, and gives every later
phase its "before". Benchmarks: the new group's numbers on `main`, which
become the baseline.

**Phase 1: O(1) snapshot with in-snapshot fence epochs** (§6.1). Removes
the fallback re-plans, the eager sample, both map clones, and the node
list clone. Value on its own: the largest estimated payoff, and it
removes the O(N) work held under the read lock in two of the three
places. Benchmarks: `plan_fleet_scaling` before and after, plus the
existing `plan_under_fill` and `candidate_construction` groups to show
no regression.

**Phase 2: budgeted verify-as-you-walk candidates** (§6.2), plus
`AvailableNodes::by_id` replacing `node_lookup`. Value on its own: makes
`build_plan` flat in node count for truncatable tasks and removes the
third O(N) read-lock hold. Independent of Phase 1 (it only needs the
by-id map, which Phase 2 can add itself if Phase 1 has not landed).
Benchmarks: `plan_fleet_scaling` and `candidate_construction` before and
after.

**Phase 3: re-measure on AWS and close the residual.** Re-run the 1k-10k
`web_fleet` ladder at defaults. Read `apply`, the read-model lock waits,
coalescer wait, outside-planning time, and the leader profile. Fix only
what the data names. Candidates already identified: index cells as
sorted `Vec<String>` (H4), anything left holding the read-model lock for
O(N), or a fleet walk in the eval handler. Each fix gets its own
before/after. If nothing material remains, Phase 3 is the re-measure
and a findings note.

**Phase 4: the eval outside `plan()`** (§ 13). Added after the 20k-node
AWS run showed 95% of each eval's wall time outside the plan phases.
Instruments the whole eval, removes the per-plan `node_lookup` and node
list clone (calls entry 12, now superseded by entry 11), and indexes the
O(jobs) SQL scans on the eval and submit paths. Value on its own: every
eval-path cost that grew with the fleet locally is gone, and the next rig
run names the rest. Benchmarks: `plan_fleet_scaling` extended to 20k and
50k nodes with an `eval_cycle` cell, before and after.

## 9. Testing plan

**Phase 0**
- Unit: `discarded_attempt` is recorded when a test forces the lazy
  fallback (mutate between snapshot and submit, the existing fallback
  test's setup) and `total` ≥ the sum of phases.
- Unit: the lock-wait histogram records one sample per acquisition.
- `benchmarks/aws/driver/tests` and the `cell_summary.py` fixtures cover
  the two derived numbers.

**Phase 1**
- The whole fence suite stays green unmodified, including
  `concurrent_plans_cannot_oversubscribe_a_node`, the
  `plan_submission_rejects_stale_epoch_*` tests, and
  `candidate_version_vector_matches_per_scope_reads_test`.
- New: a plan whose snapshot is taken, followed by a competing commit to
  one of its nodes, is rejected by the CAS (proves the snapshot epochs
  are the capacity-read epochs and not later ones).
- New: snapshot isolation. Writes after a snapshot are invisible to it,
  and the model sees them.
- New: under a background publisher, `discarded_attempt` stays zero and
  no node is oversubscribed (the concurrent bench's correctness check,
  as a test at small scale).
- Raft suites and the scheduling-index equivalence property stay green.

**Phase 2**
- Property test: for untruncated queries, `walk_verified` returns the
  same set as today's query-then-verify.
- Statistical test, alongside the existing one for
  `truncate_within_boundary_cell`: across many draws, boundary-cell
  members are chosen uniformly, and whole ranks are always kept.
- Regression: the one-slice concentration test from phase 5 stays green.
- Selectivity edge: a constraint matching 1% of the shard still returns
  the full budget when enough nodes exist.

**Every phase:** `cargo test --all --locked`, `cargo clippy --all -- -D
warnings`, and before/after numbers for the named Criterion groups on
the same host, recorded in `benchmarks/scheduler/README.md`.

## 10. Rollout and backward compatibility

- Phases 1 and 2 are default-on with no escape hatch. Both are internal
  representations that produce the same plans under the same fence, and
  a kill switch would mean keeping two snapshot implementations and two
  walk implementations alive (calls entry 8). The rollback is the
  previous binary.
- The existing off-ramps stay: `indexes: { fit: false }` and
  `index_min_nodes: 0` still turn the index off entirely.
- `lazy_version_sample` in an existing config keeps parsing, does
  nothing, and warns. No operator action is required.
- Mixed-version manager fleets are unaffected. Nothing replicated
  changes shape.
- Public docs updated with each phase: `docs/scheduler-and-eval-loop.md`
  (snapshot and fence sample, retired key), `docs/scheduling-indexes.md`
  (walk cost and the selective-constraint note), `docs/metrics.md`,
  `docs/testing-and-benchmarks.md` (new bench group).

## 11. Open questions

- **May we add `imbl`?** Lean yes. It is the smallest diff that makes
  the snapshot O(1), and the sharded fallback is ready if not.
- **How high is the fallback rate really?** Lean ~0.5-0.85 at 10k.
  Phase 0 answers it. If it is low, §5.1's payoff shrinks and Phase 2
  may deserve to go first.
- **Is the 10k leader CPU planning or heartbeats?** Lean planning, since
  per-eval planning time grew 2.75× while heartbeat volume grew 2×.
  Phase 0's profile answers it.
- **Does fill matter at all at 5 per node?** Lean no, after the
  2026-09-02 per-job index. The grid's fill column answers it.

## 12. Acceptance criteria

- **Phase 0:** the AWS archive for a 10k cell contains `eval_handle`,
  fallback rate, index outcomes, read-model lock waits, and node-list
  rebuilds. `cell_summary.py` prints outside-planning and glue.
  `plan_fleet_scaling` runs, prints per-phase means for every cell, and
  its concurrent cell shows `total` growing more than 2× from 5k to 10k.
- **Phase 1:** in `plan_fleet_scaling`, `available_nodes`,
  `capacity_snapshot`, and `version_sample` each stay under 0.1 ms and
  grow less than 1.2× from 1k to 10k nodes. `discarded_attempt` is zero
  in the concurrent cell. The fence suite is green.
- **Phase 2:** `build_plan` for the api-shaped job grows less than 1.5×
  from 1k to 10k nodes at fixed fill. `candidate_construction` shows no
  regression. Equivalence and uniformity tests are green.
- **Phase 3:** on AWS at defaults, the 10k cell's per-eval
  `eval_handle` mean is within 1.5× of the 5k cell's, or the findings
  note names the remaining term with evidence and a TODO item.

## 13. Phase 4: the eval outside `plan()` (2026-10-10)

> **Judgement calls:** entries 5, 6, 11, 13, 14, 15, and 16.

### 13.1 What the 20k cell showed

At 20,000 nodes (`web_fleet`, 5 allocations per node, binary with Phases
1-2) throughput fell to 89 alloc/s, p99 931 s. The leader was CPU-bound
(about 90% of 4 vCPUs). The eval queue peaked near 15.5k and drained at
about 15 evals/s, and `legion_eval_handle_seconds` for `job_submit` was
0.25-0.80 s against a plan `total` of 0.042 s. So almost all of an eval
was outside the instrumented plan phases. A live `perf` profile of the
leader put 21% of its CPU on the main thread, all of it in
`Store::jobs_with_unrolled_latest_config`, and named per-eval hot spots
on the tokio workers: `available_node_arcs`, constraint attribute lookups,
`serde_json::Map` clones, SipHash, `build_plan`, and SQLite statement
parsing.

### 13.2 What grows with the fleet, ranked

Measured locally (M-series laptop) with `plan_fleet_scaling`'s new
`eval_cycle` cell and a throwaway probe that builds the rig's shape (20k
nodes, 16.7k jobs, 100k allocations) and times each step.

1. **O(jobs) SQL scans, the largest local term and super-linear in fleet
   size** (the benchmark grows jobs with nodes). `job_config` had no index
   on `job_id`, `name`, or `created_at`, and `job_update` none on
   `to_job_config_id`:
   - the plan apply's own-usage read (`own_allocation_usage_tx`,
     `src/storage/store.rs`), run on every fresh plan, under the write
     connection, scanned `job_config` for the job's configs. At 4,166 jobs
     `apply` was 1.45 ms and the eval cycle 2.7 ms. After the index they
     are 0.67 ms and 1.3 ms, flat out to 16.7k jobs (1.38 ms).
   - the per-eval previous-config lookup (`previous_job_config_id`) and
     RunJob's current-config read (`GET_JOB_CONFIG` by name) scanned
     `job_update` and `job_config`.
   - the status checker's reconcilers, on the main thread, every tick:
     `jobs_with_unrolled_latest_config` took **18.9 s** per call at 16.7k
     jobs (O(jobs²) through the correlated `LATEST_CONFIG_FOR_JOB`
     subquery), and `sync_missing_task_volume_claims` 9.5 s. That is the
     pinned main thread. With the `job_config(job_id, created_at)` index
     they take 16 ms and 86 ms. Those two are fixed in a parallel change
     (calls entry 14).
2. **The per-plan `node_lookup` map and node-list clone**
   (`Scheduler::build_plan`, `Store::available_node_arcs`). Hypothesis
   confirmed as a real O(fleet) term, refuted as the cause of the
   collapse. Timed alone: 99 µs at 5k nodes, 208 µs at 10k, 474 µs at 20k,
   and 1.76 ms at 50k, plus a 17-240 µs list clone, every plan. At the
   rig's roughly 10× slowdown that is about 5 ms per eval at 20k, which is
   small next to 250-800 ms.
3. **Per-eval SQLite reads the read model already answered.** `dispatch`
   read every placed node's row (with a full JSON parse) and the job's
   task configs from SQLite, and the batch follow-up read the task
   configs again. About 0.45 ms of a 2.7 ms eval at 5k, on the read pool
   that heartbeats also use.
4. **Statement parsing.** Several eval-path statements used
   `query_row`/`execute`/`prepare` with a literal, so SQLite parsed them
   on every call: the previous-config lookup, `get_node`, task-config and
   allocation reads, the eval insert and band lookup, the scheduler-event
   insert, and the leader's changeset projection reads (`GET_NODE`, task
   config, scheduler event).
5. **Deep clones on the eval path.** `ResourcePool::from_node` cloned the
   node's whole `config_attrs` map for every node an apply re-checks, and
   `tasks_for_plan_from_store` cloned each task config it already owned.
6. **What is not per-eval O(fleet).** The capacity snapshot, the fence
   sample, and the index walk are flat (Phases 1-2). The read-model lock
   showed no contention single-threaded. A `node_join` eval loaded every
   node row to test one id (rare, fixed anyway).

What this phase could not reproduce locally is contention: single-threaded
evals cost 1-2 ms at every fleet size after the fixes, while the rig spent
hundreds of milliseconds. The remaining candidates are the blocking pool
shared with about 8k heartbeat `spawn_blocking` calls per second,
conflict-retry backoff sleeps, and CPU saturation. The new phases measure
each of them directly.

### 13.3 What changed

- **Cached node set.** `SchedulingReadModel::available_nodes_cache` holds
  `Arc<AvailableNodes>` (id-sorted list plus by-id map), returned by
  `Store::available_nodes()` as one `Arc` clone. Every `nodes` mutation
  goes through `nodes_changed()`, which drops it and bumps `nodes_epoch`.
  The rebuild runs off the write lock and installs only if the epoch is
  unchanged. Full reloads (`refresh_read_model`, so snapshot installs and
  restores) carry the epoch forward. `build_plan` uses the set as its
  lookup; plans with exclusions share it unless an excluded node is in it
  (`AvailableNodes::without`).
- **Indexes** (migration): `job_update(to_job_config_id)` and
  `job_config(name, version DESC)`, next to the parallel change's
  `job_config(job_id, created_at)` that the apply's own-usage read uses.
- **Read model instead of SQLite** in dispatch (`Store::cached_node`,
  `Store::shared_task_configs_for_job_config`) and in the batch follow-up;
  the read model's task configs are `Arc`'d so readers stop deep-cloning.
- **`prepare_cached`** for the statements in item 4.
- **Instrumentation.** `legion_eval_phase_seconds{kind, phase}`
  (`queue_wait`, `scheduler_init`, `prepare`, `plan`, `conflict_backoff`,
  `dispatch`, `finish`), `legion_eval_queue_round_seconds{op}`,
  `legion_store_read_model_lock_wait_seconds{mode}`, and
  `legion_scheduler_node_list_rebuilds_total`. The plan histogram gains
  `job_lookup`, `speculative_dispatch`, and `post_apply`, and observes
  rejected attempts. `benchmarks/aws/scripts/collect.sh` scrapes them.

### 13.4 Results

`plan_fleet_scaling`, fill 5, same laptop, before and after binaries run
back to back. `eval_cycle` is claim + handler + settle per eval.

| Nodes | `plan_and_apply` before | after | `eval_cycle` before | after |
| --- | --- | --- | --- | --- |
| 1,000 | 0.70 ms (build 171 µs) | 0.62 ms (build 172 µs) | 1.39 ms | 1.13 ms |
| 5,000 | 0.89 ms (build 318 µs) | 0.69 ms (build 240 µs) | 1.45 ms | 1.00 ms |
| 10,000 | 0.97 ms (build 404 µs) | 0.77 ms (build 292 µs) | 1.78 ms | 1.34 ms |
| 20,000 | 1.23 ms (build 584 µs) | 0.88 ms (build 339 µs) | 1.78 ms | 1.12 ms |
| 50,000 | 3.01 ms (build 2,069 µs) | 1.01 ms (build 398 µs) | 4.90 ms | 1.63 ms |

The bench holds jobs near constant, so it shows the node term only. The
jobs term shows in the rig-shaped probe: eval cycle 2.7 ms at 4,166 jobs
before the indexes, 1.3 ms after, and 1.4 ms at 16.7k jobs.

### 13.5 Deferred

- **The rig's remaining ~95%.** Read `legion_eval_phase_seconds` on the
  next 10k/20k run before changing anything else. If `queue_wait`
  dominates, give eval handlers their own threads instead of the shared
  blocking pool. If `conflict_backoff` does, revisit the retry policy.
- `sync_missing_task_volume_claims` still does one task-config query per
  active job per checker tick (TODO.md).
- In-place rollout ticks build an O(fleet) exclusion set and so a filtered
  node-set copy (`src/manager/rollout.rs`, TODO.md).
- `build_plan` still grows mildly with nodes (240 → 398 µs from 5k to 50k);
  not located.

### 13.6 The parallel leader-side change (2026-10-10)

Done alongside Phase 4, from the live `perf` profile of the 20k leader.

- **Missing indexes** (migration): `job_config(job_id, created_at DESC)`,
  `job_config(job_id, version DESC)`, and `rollout(to_job_config_id)`.
  Locally, at 16,668 jobs, the `jobs_with_unrolled_latest_config` query
  went from 60.4 s to 0.031 s, and a latest-config point lookup got about
  24x cheaper. On the rig the 10k `web_fleet` cell went from 322 to 537
  alloc/s, with p99 down from 112 s to 65 s, and the 10k storm drill
  replaced 99.65% of allocations in 35 s instead of 114 s.
- **Cached heartbeat statements.** `allocs_for_node`,
  `stop_notices_for_node`, and `volumes_to_destroy_for_node` use
  `prepare_cached`. The read model is not used for heartbeats (calls entry
  16).
- **Lost-node replacement could leave a job short permanently** (calls
  entry 15). This was the 22 missing replacements in that storm drill. The
  fix retries in place, then hands off to a `job_reschedule` eval.
- **Rig.**
  - `legion-loadgen --rate` and `driver.py run --submit-rate` give an
    open-loop paced cell, so latency can be measured below capacity
    instead of as burst drain time.
  - `run-campaign.sh` refuses a storm step that is not the last step.
  - `worker_instances` skips instances that have no private IP yet.

