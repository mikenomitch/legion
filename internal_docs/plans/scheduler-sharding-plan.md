# Sharded Scheduling (shard-local claim, shard-local read model)

**Status: PLANNED** — nothing here has shipped. This graduates the
in-process half of **D2** from
[`scheduler-architecture-review-plan.md`](scheduler-architecture-review-plan.md),
which recorded "keep the single active scheduler, design the seam" and
deferred the escalation. Phase 0 is a measurement phase and is explicitly
allowed to kill the later phases. Deliberately deferred: D2 step 2
(cross-manager planners on follower read models) stays deferred and still
waits on AWS-test data — this plan multiplies scheduling *within* one
process and is designed so that step does not have to be re-litigated.

> **Judgement calls:** [`../calls/scheduler-sharding-plan_calls.md`](../calls/scheduler-sharding-plan_calls.md)

## Problem

We have spent a measurement campaign burning down per-plan cost, and it
worked. From [`benchmarks/scheduler/README.md`](../../benchmarks/scheduler/README.md):
candidate construction is index-served; the per-plan snapshot was fixed
twice (−55% and then 4.5–5.8x); the O(fleet) `version_sample` term is gone
(D4, ~50x at 1k nodes and no longer growing with the fleet); plan
submission and apply are one fused quorum commit; applies group-commit
(D5). What is left is not per-plan cost.

**The ceiling is contention on shared state, and we measured it.** D5's
group commit cut p50 plan latency 2.4–3x on the contended cells and
returned only 16–55% throughput — because the conflict rate is 1.25+
retries per plan on `steady-small` and is *identical with the lever on and
off*. Batching neither causes nor fixes it. Four concurrent `bin_pack`
planners pick the same argmax node, and re-planning burns the latency the
batch saved.

Underneath that, three things are shared by every eval worker:

- **One queue, and every claim is a consensus round.** `max_threads` eval
  workers ([`src/manager/daemon.rs:1126`](../../src/manager/daemon.rs))
  all call `store.claim_eval()`, which is
  `replicated(Lane::Scheduler, "claim_eval", …)`
  ([`src/storage/store.rs:11158`](../../src/storage/store.rs)) — in raft
  mode a full quorum round *per claim*, before any planning happens, over
  a single `UPDATE … WHERE id = (SELECT … ORDER BY priority DESC, id ASC
  LIMIT 1)` that every worker targets simultaneously.
- **One read model.** Every planner builds its snapshot from the whole
  fleet, so per-plan work tracks cluster size even where the candidate set
  does not.
- **One write mutex at apply**, arbitrated by per-node epoch CAS.

Adding threads past the contention point does not add throughput. That is
the wall this plan is about, and it is the same wall every incumbent hit:
the architecture review already recorded that "Gödel/ByteDance sharded the
scheduler to reach 5k pods/sec — evidence that single-scheduler is the
eventual bottleneck for everyone."

## Prior art

- **Nomad.** Many scheduler workers plan optimistically against a shared
  in-memory state store; a single leader **plan queue** serializes apply
  and rejects conflicting plans. Aggregate 3,750 placements/sec at C1M.
  The important structural point: planning is parallel, *apply is not*,
  and correctness comes from the arbiter rather than from partitioning.
- **Kubernetes.** One leader-elected scheduler with an internal
  parallelism knob and `percentageOfNodesToScore` to bound work per pod.
  Gödel (ByteDance) sharded it to ~5k pods/sec.
- **Tensorlake.** Replaced a single-threaded placement loop (~4kHz tick,
  ~600 placements/sec per host) with a "sharded cooperative scheduler":
  each shard owns a bucket of hosts and runs its own placement loop, with
  cross-shard work only at rebalance (~every 90s). Their stated mechanism
  is cache residency — "each shard can make placement decisions with
  roughly 1/32nd of the global state, which fits in L2 cache, which turns
  the hottest loop in the system from memory-bound to CPU-bound" — and
  their published figure is >24,000 sandboxes/sec scheduled, ~32× a
  per-shard rate close to the pre-shard per-host one. Sharding bought
  them parallelism, not a faster decision.

**What we take:** partition the fleet so a planner touches a slice; keep
rebalancing rare and off the plan path; expect the win to be parallelism
and reduced collision, not a faster inner loop.

**What we reject:** partitioning as a *correctness* mechanism. Tensorlake's
shards own their hosts; ours will not. Apply stays a single arbiter with
today's epoch CAS, so a sharding bug can cost throughput or packing
quality but can never double-place a node. This is judgement call
[#2](../calls/scheduler-sharding-plan_calls.md).

**What we take from their reporting, in the negative:** their 24,000/sec
is a placement-decision rate, published without the delivery or start rate
beside it. See [Measurement](#measurement-what-number-this-produces).

## Jobs to be done

**Cluster operator**
- When my cluster grows from hundreds of nodes to tens of thousands, I
  want scheduling to keep up by using the machine I already gave it, so
  capacity planning for the control plane stays boring.
- When one team's workload floods the queue, I want other teams' work to
  keep being scheduled, so a noisy neighbor is a slowdown and not an
  outage.

**Job author / app developer**
- When I submit a job at the same moment as everyone else, I want it
  placed in a time that does not depend on how many unrelated jobs were
  submitted alongside it.

**On-call responder**
- When scheduling is slow, I want to see whether the work is queued,
  contended, or retrying, so I can tell a capacity problem from a
  contention problem.

**Maintainer**
- When I change the scheduler, I want a benchmark that isolates
  contention from per-plan cost, so I can tell which of the two a change
  actually moved.

*Not in scope:* job authors do not see shards. No jobspec field, no
placement semantics change, and `job plan` output is unchanged except for
a diagnostic line. If a shard is ever visible in a jobspec, this design
has failed.

### User scenarios

1. **Mass submit on a 5,000-node single-pool cluster.** 500 jobs arrive
   at once. Today the eval workers serialize on `claim_eval` and then
   collide on the same argmax nodes. After: claims are shard-local (no
   contention on the queue head) and planners in different shards cannot
   collide at all, because their candidate slices are disjoint.
2. **Two pools, one hot.** A batch pool is churning; a service pool is
   quiet. Today the batch churn sets the queue depth every service eval
   waits behind. After, they are different shards.
3. **A shard goes hot.** One shard's queue depth grows while others idle.
   Rebalance moves node groups between shards on a slow timer; the
   operator sees per-shard depth in `legion operator scheduler -j`.

## Goals

- Remove `claim_eval` from the contended path: a worker claims from its
  own shard.
- Make per-plan snapshot and candidate work track the *shard*, not the
  fleet.
- Reduce the *rate* of optimistic-concurrency collisions by making
  concurrent planners work on disjoint node sets where the workload
  permits it.
- Keep the apply arbiter, the epoch CAS, and every correctness property
  exactly as they are.
- Produce an honestly-scoped aggregate placements/sec number.

## Non-goals

- Cross-manager scheduling (D2 step 2). Still deferred, still gated on
  AWS-test data. This plan is in-process.
- Replacing SQLite (D3, rejected — operability is the product).
- Fixing *within-shard* collisions. Two planners in the same shard can
  still pick the same argmax node; that is
  [`late-binding-plan-apply-plan.md`](late-binding-plan-apply-plan.md)'s
  job, and the two compose (see [Sequencing](#sequencing-against-late-binding)).
- Any user-visible placement semantics change. Sharding must not alter
  which node a job lands on in a quiet cluster.
- Per-shard fairness or quotas. Bands and namespace caps already exist.

## Background — what exists today

| Piece | Where | Behavior |
| --- | --- | --- |
| Eval workers | `src/manager/daemon.rs:1126` | `max_threads` workers, one shared `Notify`, one-eval-at-a-time each |
| Claim | `src/storage/store.rs:11158` | Replicated scheduler-lane op; global queue head; collapses subsumed work |
| Leadership | `src/manager/daemon.rs:1188` | Leader-only; followers idle and inherit the queue on election |
| Planner | `src/manager/scheduler.rs` (7,297 lines) | One `Scheduler::new` per eval; settings parsed once at construction |
| Candidate index | `src/scheduling_index.rs:149` | **Already defaults to `partition_by: ["pool"]`** |
| Apply | `Store::submit_and_apply_plan` | Fused insert+apply, group-committed, per-node epoch CAS |
| Pools | ACL + config concept | TODO records: "nothing scheduling-side consumes them" |

The last two rows are the design. The candidate index already partitions
by pool, and pools are already the axis operators reason about — but
nothing in the scheduling *loop* consumes them. This plan consumes them.

## Proposed design

### 1. Shard key: pool, subdivided by node hash

A shard owns a set of **node groups**. A node group is
`(pool, hash(node_id) % subdivisions)`.

Pool alone is the intuitive key and the wrong one by itself: most Legion
clusters have exactly one pool (`default`), and a pool-only scheme gives
them one shard and no win at all. Subdividing by a hash of the node id
inside the pool fixes the common case, and keeps the operator-meaningful
boundary intact for clusters that do use pools. Judgement call
[#1](../calls/scheduler-sharding-plan_calls.md).

`subdivisions` is derived from node count, not configured per pool:
one group per shard-worth of nodes, floor 1.

### 2. Evals carry a shard

`eval` gains a nullable `shard_id`, assigned at enqueue:

- The eval's job binds a pool (via constraints) → the pool's shard.
- It does not, or it spans pools → `NULL`, the **global shard**.

The global shard is claimed by any worker when its own shard is empty, and
its plans run against the full fleet exactly as today. Unbindable work
therefore gets today's behavior, not a broadcast and not a stall.

### 3. Shard-local claim

`claim_eval` gains a shard predicate:

```sql
WHERE status = 'pending' AND ready_at <= :now
  AND (shard_id = :shard OR (:take_global AND shard_id IS NULL))
```

Workers are assigned shards round-robin at spawn. A worker whose shard is
empty takes global work, then steals from the fullest shard — work
stealing keeps a skewed cluster from idling cores, and stealing is safe
precisely because shards are not a correctness boundary (§2 of Prior art).

This alone removes the queue-head contention: in raft mode, N workers
stop issuing N competing consensus rounds against the same row.

### 4. Shard-local read model slice

The planner's node snapshot is built from the shard's node groups rather
than the fleet. The scheduling index already partitions by pool, so the
candidate path largely follows; the work is in the snapshot and capacity
ledger, which today are fleet-wide.

The claim to test — and the one Tensorlake makes — is that a shard-sized
working set is cache-resident where a fleet-sized one is not. We should
expect a smaller effect than they report, because our per-plan cost is
already index- and snapshot-bounded rather than scan-bounded. Phase 2's
benchmark is where that expectation is confirmed or dropped.

### 5. Rebalance

A slow timer (default 90s) moves node groups between shards when queue
depth or node count skews past a threshold. Never on the plan path. A
rebalance is a read-model reassignment, not a data move: no allocation
changes, no eval is re-planned, in-flight evals finish under their old
shard.

### 6. Surface

```jsonnet
{
  schedulerSettings: {
    shards: {
      // 'auto' derives shard count from eval-worker threads; 1 disables
      // sharding entirely and is the default until measured.
      count: 'auto',
      key: 'pool',              // the only supported key in v1
      rebalanceInterval: '90s',
      workStealing: true,
    },
  },
}
```

Operator visibility:

```console
$ legion operator scheduler -j
{"ok":true,"data":{"shards":[
  {"id":0,"pools":["default"],"groups":2,"nodes":2481,"queue_depth":3,"conflicts_per_plan":0.11},
  {"id":1,"pools":["default"],"groups":2,"nodes":2519,"queue_depth":0,"conflicts_per_plan":0.09}],
 "global_queue_depth":1,"last_rebalance":"2026-07-31T10:02:11Z"}}
```

`conflicts_per_plan` on that output is deliberate: contention is the thing
this plan exists to reduce, so it is the thing the operator can see.

## Proto, struct, config, and persistence changes

- **Store**: `eval.shard_id` column + migration (nullable, so existing
  rows are global-shard and behave exactly as today); shard predicate in
  `claim_eval`; shard-scoped snapshot builders.
- **Config**: the `shards` block above in `schedulerSettings`, defaulting
  to disabled.
- **Proto**: no new RPC. `GetObservability` gains the per-shard block
  shown above, fully commented.
- **Metrics**: `legion_scheduler_shard_queue_depth{shard}`,
  `legion_scheduler_shard_conflicts_per_plan{shard}`,
  `legion_scheduler_shard_steals_total`,
  `legion_scheduler_rebalance_total`.
- No job-config fields, so `configs/jobs/maximal.jsonnet` is untouched.

## Implementation phases

**Phase 0 — a benchmark that isolates contention.** Extend the existing
concurrency harness (`LEGION_BENCH_CONCURRENCY`, already used for the D5
A/B) to report conflicts-per-plan and claim-wait as first-class series,
across thread counts and node counts. We have the conflict number today
only as a footnote in a D5 table; this makes it the dependent variable.
**This phase may kill the plan**: if claim contention is small and
conflicts are dominated by same-argmax collisions *within* any plausible
shard, then late binding is the whole fix and sharding buys little.

**Phase 1 — shard-local claim.** `shard_id`, shard assignment at enqueue,
shard predicate, work stealing, per-shard metrics. Read model still
fleet-wide. Independently valuable: removes N-way consensus contention on
the queue head, which is measurable on its own.

**Phase 2 — shard-local read model.** Snapshot and capacity ledger built
per shard. Independently valuable: per-plan cost decouples from fleet
size.

**Phase 3 — rebalance and `auto`.** Skew detection, group movement,
derived shard count.

**Phase 4 — publish**, and re-open D2 step 2 with the data. If in-process
sharding gets us within reach of the aggregate we want, step 2's wire
surface stays deferred — which was the review's hope.

## Sequencing against late binding

These two plans attack the same measured ceiling from opposite ends:

- **Sharding reduces the collision *rate*** — planners in different shards
  cannot pick the same node.
- **Late binding reduces the *cost* of a collision** — a plan carries
  ranked candidates and binds to the first whose epoch is unchanged,
  costing a rank instead of a retry.

Neither subsumes the other, and sharding cannot fix within-shard
collisions at all. The honest lean is **late binding first or
concurrently**: the measured pathology (4 planners, same argmax, 1.25+
retries/plan) happens on a 12-node cluster that would be one shard under
any sharding scheme, so sharding would not have moved that cell. Phase 0
of this plan should be run *after* late binding's Phase 0 so the
conflicts-per-plan baseline already reflects it. Recorded as open question
[#1](#open-questions).

## Testing plan

- **Property — placement equivalence.** For a quiet cluster (no
  concurrency), placements chosen with sharding on must be *identical* to
  placements chosen with it off, for the same fleet and jobspec. Sharding
  is a contention device; it must not change where things land when
  nothing is contending. This is the centerpiece test, modeled on the
  scheduling-index equivalence property.
- **Property — packing quality under concurrency.** With sharding on,
  allocations-per-node distribution must stay within a stated bound of the
  unsharded run (the same quality-bound shape the bounded-candidate-scoring
  plan used). Bin-pack across disjoint slices packs slightly worse; the
  bound makes "slightly" a number.
- **Unit**: shard assignment for pool-bound, multi-pool, and unbound
  evals; hash subdivision stability when nodes join and leave; steal
  ordering; rebalance leaves in-flight evals alone.
- **Integration**: raft leadership change mid-rebalance; a pool drained to
  zero nodes; single-pool cluster with `count: 'auto'`; every existing
  scheduler suite green with `shards.count: 1`.
- **Regression**: the full SLO gate (`tests/scheduler_benchmark_slo.rs`)
  and the scale matrix, both arms.
- **Benchmark**: Phase 0's harness per phase, with before/after tables in
  `benchmarks/scheduler/README.md` per the AGENTS.md scheduler rule.

## Rollout and backward compatibility

- `shards.count` defaults to `1` — sharding off, today's code path — until
  the Phase 4 numbers exist. `1` remains supported forever.
- The `shard_id` migration is a nullable column; every existing eval row
  is global-shard, which is today's behavior exactly. Downgrade ignores
  the column.
- No wire change for workers, no jobspec change, no ACL change. A cluster
  can turn sharding on and off with a config reload; in-flight evals
  finish under whatever shard they claimed.
- Multi-manager clusters are unaffected: this is in-process on the
  scheduler-lane leader, and followers still idle exactly as they do now.

## Measurement — what number this produces

The number this plan aims at is **aggregate committed placements per
second**, and it is worth being precise about what that is not.

The claim that prompted this work — "more than 24,000 sandboxes per
second" — is a placement-decision rate. It was published in a product post
with no methodology, no hardware, and no delivery or start rate beside it,
by a team whose engineering posts otherwise document their fio commands
and volunteer their own caveats. Their own concrete delivery measurement,
in a different post, is roughly an order of magnitude lower. Derived from
their published architecture, 24,000/s is ~32 shards × ~750/s per shard —
essentially their pre-sharding per-host rate, multiplied by parallelism.
That is a reasonable engineering result described in a way that invites a
misread, and the misread is the part we must not copy.

So, reported together, never collapsed:

1. **`placements_committed_per_sec`** — aggregate across shards, plans
   that committed. The headline, and meaningless alone.
2. **`conflicts_per_plan`** — retries per committed plan. A throughput
   number without this is a number from a run that may have been
   thrashing.
3. **`correctness`** — expected vs actual allocations, which the existing
   harness already reports as 1.000 and which every table must keep.
4. **`packing_quality`** — allocations-per-node distribution vs the
   unsharded run, since sharding trades packing for parallelism.

Plus, on every table: scenario, fleet size, backend, host, build profile,
driver concurrency, and machine state. The benchmark README's existing
discipline — "compare phases within a run and before/after pairs measured
close together; treat absolute evals/sec as indicative" — applies
unchanged, and the AWS campaign
([`aws-comparative-benchmark-plan.md`](aws-comparative-benchmark-plan.md))
remains the only place steady-state absolutes become authoritative.

**What we will claim:** an aggregate placement rate at a stated fleet
size, backend, and host, with conflict rate, correctness, and packing
quality beside it, and a scaling curve rather than a single cell.
**What we will not claim:** a placement rate as a "tasks started per
second" rate; a number from a laptop as a fleet number; a shard-count
multiplier we have not measured end to end.

## Open questions

1. **Does late binding land first?** *Lean: yes* — see
   [Sequencing](#sequencing-against-late-binding). The alternative is to
   run both Phase 0s together and let the conflict decomposition decide,
   which is slower but better evidence.
2. **Should shard count follow eval-worker threads or node count?**
   Threads is the contention axis; nodes is the cache-residency axis.
   *Lean: threads for `auto` in v1* — contention is the measured problem
   and residency is the speculative one.
3. **Work stealing and packing quality.** A stealing worker plans against
   a shard it does not own, which is safe but reintroduces collisions.
   *Lean: allow it, measure the collision rate it adds, and make it
   togglable (`workStealing`) so the trade is an operator's to make.*
4. **Does the global shard become a bottleneck on constraint-light
   clusters?** A cluster whose jobs bind no pool puts everything in the
   global shard and gets nothing from this plan. *Lean: acceptable in v1
   and stated plainly in the public doc; the hash subdivision means a
   later version could shard global work by job id.*

## Acceptance criteria

- **Phase 0**: a committed benchmark reporting conflicts-per-plan and
  claim-wait across thread and node counts, with a baseline table in
  `benchmarks/scheduler/README.md`.
- **Phase 1**: claim-wait under N concurrent workers is flat in N where it
  is linear today, on raft1 and raft3; every existing scheduler test green
  with `shards.count: 1` and with `'auto'`.
- **Phase 2**: per-plan snapshot cost tracks shard size, not fleet size —
  shown by holding the shard size constant while doubling the fleet.
- **Phase 3**: an induced skew rebalances within one interval without
  changing any placement (equivalence property holds across a rebalance).
- **Phase 4**: a before/after table with all four Measurement series, a
  scaling curve across at least three fleet sizes, and a public
  `docs/` page covering the shard key, the defaults, work stealing, and
  the global-shard limitation from open question 4.
