# Scheduler Architecture Review for Scale (proposals)

**Status: PROPOSALS ACCEPTED / GRADUATED** — this is a proposals
document: it evaluates architectural decisions against the "fastest
scheduler at scale" goal and proposes the big levers, with costs and
benefits. No code ships from this plan directly. D4/D5/D6 graduated into
[`plan-path-scale-levers-plan.md`](plan-path-scale-levers-plan.md), D1
into [`bounded-candidate-scoring-plan.md`](bounded-candidate-scoring-plan.md),
and the locally-measurable slice of D8 into
[`heartbeat-scale-plan.md`](heartbeat-scale-plan.md). D2 (keep single
scheduler, seam designed) and D3 (keep SQLite) remain the recorded
keep-decisions; D8's architecture choice still waits on AWS-test data.

> **Judgement calls:** [`../calls/scheduler-architecture-review-plan_calls.md`](../calls/scheduler-architecture-review-plan_calls.md)

## Problem

The measured-optimization loop
([`scheduler-performance-plan.md`](scheduler-performance-plan.md)) burns
down costs *within* the current architecture. But some costs are the
architecture: decisions that are fine at 1k nodes and wrong at 50k. If
Legion wants to be the fastest scheduler at scale — and to say so publicly
with the AWS test
([`aws-comparative-benchmark-plan.md`](aws-comparative-benchmark-plan.md))
— the load-bearing decisions need deliberate review now, while changing
them is still cheap.

The measured baseline this review reads from (2026-07-29 phase data, debug
and release matrices in `benchmarks/scheduler/README.md`):

- `apply` (plan submission commit) is the largest or second-largest phase
  in every cell, on every backend; raft quorum commit ~3x's it.
- `build_plan` dominates constraint-heavy cells; scales with fleet size on
  the scan path, near-flat with indexes armed.
- `version_sample` (the optimistic-concurrency fence sample) is O(fleet)
  per plan — ~1.2 ms at 1k nodes, which projects to ~12 ms at 10k on the
  same curve.
- Snapshot phases (`available_nodes` + `capacity_snapshot`) are ~1 ms at
  1k nodes per plan, rebuilt for every plan.

## How the incumbents made these calls

- **Nomad** scores a *sample* of feasible nodes (min 2, log2 n, cap 100)
  per placement; runs many scheduler workers across servers, each
  producing plans optimistically; a single leader **plan queue**
  serializes apply with conflict rejection. That architecture is what
  C1M's 3,750 placements/sec aggregate came from. State is in-memory
  (go-memdb) with the raft log as truth.
- **Kubernetes** caps filtering via `percentageOfNodesToScore` (~10% at 5k
  nodes, floor 50); one active kube-scheduler (leader-elected) with an
  internal parallelism knob; state in etcd, watched into an in-memory
  cache. Gödel/ByteDance sharded the scheduler to reach 5k pods/sec —
  evidence that single-scheduler is the eventual bottleneck for everyone.
- **Legion today**: exact scoring over all (or index-narrowed) feasible
  nodes; one active manager schedules; parallel eval workers in-process
  with partitioned-epoch optimistic concurrency; state authoritative in
  SQLite (single writer), read model in memory, raft/streaming
  replication as configured.

## Jobs to be done

- **Maintainer**: know which big changes are worth making, in what order,
  and which tempting ones to refuse — before sinking months.
- **Operator**: keep Legion's operational simplicity (one binary, SQLite
  you can query, obvious failure modes) through whatever changes land.
- **The public claim**: an architecture whose measured scaling curves
  support "fastest at scale" without asterisks that embarrass us.

## Goals

- Evaluate every scale-relevant architectural decision explicitly:
  keep / change / defer, each with costs, benefits, and a product-goal tie
  (simplicity, extensibility, performance).
- Propose the changes worth making as ranked, phased items, each sized and
  risked, ready to graduate into standalone plans.

## Non-goals

- Shipping any of it here.
- Re-litigating settled product decisions that are not scale-relevant
  (jsonnet configs, plugin system, CLI shape).

## The decisions, reviewed

### D1. Exact scoring over all feasible nodes — **change (opt-in sampling), highest-impact**

Every placement scores every feasible node (post-index). Nomad scores
log2(n); K8s scores ~10%. This is THE reason incumbent per-placement cost
is flat while ours grows with fleet size.

**Proposal**: bounded candidate scoring. After feasibility (scan or
index), when candidates exceed a threshold, score a sample: the threshold
and sample size configurable, `schedulerSettings.score_sample: { min_nodes:
1024, sample: 128 }` shape. Below the threshold, behavior is exactly
today's. Sampling strategy: uniform over feasible candidates for spread;
for bin_pack, a power-of-two-choices variant (sample k, take best fit)
preserves packing quality surprisingly well (known result from the
balls-into-bins literature; validate with the equivalence property test
relaxed to a quality bound).

- **Benefit**: per-placement cost flattens — the one change that makes
  100k-node placement latency structurally competitive. Composes with the
  scheduling index (index narrows, sample bounds scoring).
- **Cost**: placement quality becomes approximate above the threshold;
  "why this node" explanations weaken; a new tuning knob. Bin-pack
  tightness loss must be measured (add a packing-quality metric to the
  benchmark runner: allocs-per-node distribution vs exact).
- **Why it wins**: performance goal, directly; the differentiator framing
  stays honest — *exact when it's cheap, bounded when it matters*, with
  the threshold printed in `job plan` explain output.
- **Size**: medium (scheduler-local; no store/wire change). Graduates to
  its own plan with the quality-bound test as the centerpiece.

### D2. Single active scheduler — **keep now; design the seam for parallel apply**

Nomad's plan-queue architecture (many optimistic schedulers, one apply
arbiter) is how you get aggregate throughput; our partitioned epochs are
*already* that arbiter's admission rule, applied under one process's write
mutex. Two escalation steps exist:

1. **In-process parallel apply batching** (D5, below) — cheap, keeps one
   process.
2. **Cross-manager scheduling workers**: followers run plan_only against
   their (replicated, slightly stale) read model, ship plan submissions to
   the leader, leader applies with the same epoch CAS. The fence semantics
   need zero change — that is what optimistic concurrency is for.

- **Benefit**: step 2 multiplies scheduler CPU by manager count — the C1M
  shape; also the only route to saturating the raft pipeline (today one
  process both plans and proposes).
- **Cost**: step 2 is a wire surface (plan-submission RPC), staleness
  handling on follower read models, conflict-rate growth under contention
  (epochs make it correct, retries make it slow if hot), and real
  distributed-debugging burden. Operational simplicity takes a hit.
- **Call**: keep single-scheduler until D1 + D5 + the measured
  optimizations stop paying; the epoch design means the door stays open
  with no rework. Revisit with AWS-test data — if Legion's single-manager
  placements/sec × 3 already beats Nomad's aggregate on the same
  hardware, we may never need it; the honest projection goes in the AWS
  report either way.

### D3. SQLite as the authoritative hot-path store — **keep, with the write path narrowed**

The tempting change is Nomad's shape: authoritative in-memory state,
log-as-truth, snapshot to disk. Apply cost would drop from ~1.5-6 ms
(measured, local/raft) toward microseconds.

- **Benefit**: order-of-magnitude apply-phase win; removes the single
  biggest measured cost.
- **Cost**: it deletes Legion's most operator-loved property — *the
  cluster state is a SQLite file you can query, back up, and restore with
  standard tools* — and rewrites crash recovery, the read model, the
  raft state machine, streaming persistence, and every migration
  assumption. Highest-risk change on this list by far.
- **Call**: **keep SQLite**, attack the same cost from three cheaper
  directions: batch/coalesce applies (D5), keep shrinking per-apply work
  (measured-optimization loop), and cut the *number* of applies (D6).
  Re-open only if, after those, apply still caps the AWS-test numbers —
  the plan records the decision so the AWS report can say "we chose
  operability; here is what it costs and here is the roadmap if it ever
  matters."
  (Simplicity goal outranks a benchmark cell we can win other ways.)

### D4. O(fleet) optimistic-concurrency sample — **change (targeted)**

`allocation_version_vector_for_candidates` reads an epoch for every
candidate node on every plan — measured ~1.2 ms/plan at 1k nodes, linear.
The fence only *checks* the scopes the plan finally touches
(`expected_versions_for` selects job scope + affected nodes), but it
*samples* everything because affected nodes are unknown pre-planning.

**Proposal**: sample lazily — plan first against the snapshot, then
sample epochs for exactly the affected nodes, and reject if the *snapshot
generation* moved during planning (a single global read-model generation
counter bumped on every allocation mutation, checked once). The
generation check preserves the "sampled no later than the capacity read"
invariant the code comments prove; per-node epochs still do the precise
CAS at apply time.

- **Benefit**: removes an O(fleet) term from every plan; at 10k nodes
  that is ~12 ms/plan on the measured curve — bigger than build_plan.
- **Cost**: touchy correctness code (the oversubscription fence);
  a global generation adds one contended atomic; more spurious retries
  under heavy unrelated churn (generation is cluster-wide) — mitigable
  with a per-pool generation if measured.
- **Size**: small-medium, store-local. High value/risk ratio; first in
  line after this review.

### D5. One apply per plan — **change (group commit)**

Every eval's plan commits its own write transaction (and raft propose).
Measured: apply dominates; raft3 apply ≈ 4.7 ms of a 6.5 ms plan.

**Proposal**: a plan-apply coalescer on the store: applies queued within a
short window (e.g. 2-5 ms, or "whatever is queued when the previous commit
finishes" — natural batching, no timer latency when idle) commit in one
transaction / one raft propose, each plan keeping its own epoch CAS verdict
inside the batch.

- **Benefit**: amortizes fsync + quorum round-trip across concurrent
  evals; directly multiplies eval throughput under load on every backend;
  biggest win on raft3 (the deployment we recommend for HA).
- **Cost**: batch-mate failure isolation (one plan's constraint violation
  must not poison the batch — per-plan savepoints or re-run-singly on
  batch failure); slight added latency for a lone eval (bounded by the
  window); the eval loop's completion semantics need care.
- **Why**: performance at the exact point every measurement says is
  hottest, without touching the state model. Natural companion to D2
  step 1.

### D6. Rebuild snapshot + plan per job on every eval — **change (snapshot reuse), smaller**

Burst evals (churn, mass submit) rebuild `available_nodes` +
`capacity_snapshot` per job (~1 ms/plan at 1k nodes, linear).

**Proposal**: an eval-batch plan context: one snapshot (nodes + capacity
+ generation) shared by consecutive plans in a worker's claim streak,
invalidated by the D4 generation counter. Pure scheduler-side cache.

- **Benefit**: turns per-plan snapshot cost into per-batch; matters
  exactly in the storm scenarios (mass reschedule) where throughput is
  the story.
- **Cost**: staleness between batch-mates raises conflict probability
  (epoch CAS still guarantees correctness); cache invalidation logic.
- **Size**: small. Pairs with D4 (same generation mechanism).

### D7. Lanes topology (multi-raft) — **keep, promote in benchmarks**

Already built and measured (lanes keep scheduler-lane latency flat under
control/status flood — `tests/raft_perf_smoke.rs`, `docs/multi-raft.md`).
The scale matrix should run raft3 in both `single` and `lanes` topology so
the recorded numbers show what lanes buy; the AWS test runs lanes.
No change to the design.

### D8. Heartbeat/status load on the manager — **defer to AWS-test evidence**

At 10k+ real workers, heartbeats + status updates contend with scheduling
(status lane absorbs some of it). Options if it bites: heartbeat
coalescing/decimation, worker-side status batching, gossip-assisted
liveness (Serf-style, a big dependency). No local benchmark can settle
this — the AWS test's fake-worker fleet is designed to measure exactly
this pressure (its scenario matrix scales worker count independently of
allocation count). Decide with that data.

## Ranked proposal summary

| # | Change | Size | Expected win | Risk |
|---|--------|------|--------------|------|
| 1 | D4 lazy version sample + generation | S-M | kills an O(fleet) per-plan term | fence correctness (property-tested) |
| 2 | D5 apply group-commit | M | amortizes the hottest phase, 3x+ eval throughput under load on raft | batch failure isolation |
| 3 | D1 bounded candidate scoring | M | flat per-placement cost at any fleet size | placement quality (measured bound) |
| 4 | D6 eval-batch snapshot reuse | S | removes per-plan snapshot rebuild in storms | staleness→retries |
| 5 | D2 step 2: cross-manager scheduling | L | multiplies scheduler CPU; C1M shape | wire surface, ops complexity — only if data demands |
| 6 | D3 in-memory state | XL | order-of-magnitude apply | rejected for now — operability is the product |

## Phases

1. Socialize this review; confirm the keep/change calls.
2. Graduate #1/#2 (D4+D5) into a "plan-path scale levers" implementation
   plan — they share machinery and tests.
3. Graduate #3 (D1) with the packing-quality benchmark as its gate.
4. Re-run the scale matrix + AWS test; revisit D2-step-2/D8 with data.

## Testing story

Every proposal names its gate above; the shared ones: the scheduling-index
equivalence property, oversubscription/epoch-fencing regression tests, the
SLO gate, and before/after cells from `scheduler_scale_benchmark` recorded
in `benchmarks/scheduler/README.md`.

## Rollout

Nothing here ships directly. Accepted proposals carry their own rollout
sections; every one of them is designed to be flag-gated with today's
behavior as the default until measured.
