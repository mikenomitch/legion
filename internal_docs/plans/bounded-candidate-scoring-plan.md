# Bounded Candidate Scoring (opt-in score sampling)

**Status: IN EXECUTION** — graduates proposal D1 from
[`scheduler-architecture-review-plan.md`](scheduler-architecture-review-plan.md).

> **Judgement calls:** [`../calls/bounded-candidate-scoring-plan_calls.md`](../calls/bounded-candidate-scoring-plan_calls.md)

## Problem

Legion scores **every** feasible node for every placement
(`score_nodes_with_cache`, `src/manager/scheduler.rs:2743`, plus the
eligibility and best/tie passes around it — several O(feasible) passes
per placement, repeated `count` times per task). With the scheduling
index armed, "feasible" is a narrowed shard, but for weakly-constrained
jobs on a large fleet it is the fleet. This is the one remaining
per-placement cost that grows with fleet size: at 100k nodes, exact
scoring is structurally uncompetitive — Nomad scores ~log2(n) feasible
nodes, Kubernetes ~10% (floor 50).

## Prior art

- **Nomad**: `LimitIterator` — service jobs score max(2, log2 n) feasible
  nodes per placement; batch jobs even fewer. This is what made C1M's
  aggregate placement rate possible.
- **Kubernetes**: `percentageOfNodesToScore` — defaults to an adaptive
  percentage (~10% at 5k nodes, floor 50, cap 100%); scoring stops after
  the sample.
- **Balls-into-bins / power-of-two-choices**: choosing the best of k
  uniformly-sampled bins gives exponentially better load balance than
  random placement even for k=2; for bin-packing quality, best-of-k
  sampling preserves packing tightness surprisingly well for modest k.
  This is the theory backing the quality bound below.

## Jobs to be done

**Cluster operator**
- When my fleet is large enough that exact scoring costs milliseconds per
  placement, I want to cap scoring work at a fixed sample, so placement
  latency stays flat as the fleet grows.
- When I turn the cap on, I want to see the packing/spread quality cost
  measured, so the trade is a decision rather than a surprise.

**Job author**
- When I run `job plan`, I want the explain output to say when a
  placement was chosen from a sample rather than the whole fleet, so
  "why this node" stays answerable.

## Goals

- An opt-in `schedulerSettings.score_sample { min_nodes, sample }` knob:
  below `min_nodes` feasible candidates, behavior is exactly today's;
  at/above it, score a uniform sample of `sample` candidates.
- Packing-quality measurement in the benchmark runner (allocs-per-node
  distribution vs exact), so the quality cost is a recorded number.
- Sampling composes with the scheduling index (index narrows first,
  sample bounds scoring) and with `nodes_considered` (which it
  supersedes but does not remove).

## Non-goals

- Changing the default (exact scoring stays the default at any fleet
  size until AWS-test data argues otherwise).
- Adaptive/percentage sampling (K8s-style) — a fixed sample size is
  simpler to reason about; revisit with data.
- Sampling feasibility (constraints/permits are always fully enforced —
  only *scoring* is bounded; a sampled node is always a *valid* choice).

## What exists today

- `nodes_considered` (`src/manager/scheduler.rs:2292`, `:2481`): an
  existing unconditional shuffle-and-truncate knob (int or percentage),
  applied per placement for bin_pack/spread. No threshold gating — it
  applies at any fleet size, so it cannot express "exact when cheap,
  bounded when it matters".
- Selection: `select_node_id` → `select_cached_node` /
  `select_cached_node_with_jitter` (`scheduler.rs:1970-2040`) over the
  `PlanScoringCache`.
- `job plan` explain surface: `TaskIndexUsage` reports per-task candidate
  provenance (scan vs index, truncation).

## Proposed design

Manager config:

```jsonnet
{
  schedulerSettings: {
    score_sample: {
      min_nodes: 1024,   // exact scoring below this many eligible candidates
      sample: 128,       // candidates scored at/above the threshold
    },
  },
}
```

- Applied in `select_node_id` after feasibility filtering (permit,
  distinct, resource fit) — the same seam as `nodes_considered`, gated
  by `eligible.len() >= min_nodes`. Uniform sample without replacement;
  then the existing exact scoring runs on the sample. For bin_pack this
  is best-of-k (power-of-k-choices); for spread, least-loaded-of-k.
- Validation: `sample >= 2`, `min_nodes >= sample` (rejected at manager
  start otherwise, same as other `schedulerSettings` validation).
- Explain surface: task reports in `job plan` note
  `scored=<sample> of <eligible>` when sampling engaged.
- Both `score_sample` and `nodes_considered` set: `nodes_considered`
  applies first (it is a hard cap), then `score_sample` — documented,
  though combining them is not recommended.

## Config/persistence changes

One new `schedulerSettings` block (manager config only — not job
config, so `configs/jobs/maximal.jsonnet` is unaffected; the manager
example config gains the block, commented out). No proto, wire, disk, or
migration changes.

## Implementation phases

1. The knob + sampling + validation + explain note, with unit tests.
2. Packing-quality measurement in the benchmark runner + recorded
   numbers (exact vs sampled on the same scenario).

## Testing plan

- Unit: threshold gating (below → byte-identical selection to today,
  seeded; above → selection ∈ eligible, sample-size respected);
  validation errors; interaction with distinct constraints (sampled
  choice always satisfies filters — by construction, asserted anyway).
- Quality bound (the centerpiece): a property-style test that bin-packs
  a large fleet with sampling on and asserts nodes-used stays within a
  bound of the exact run (generous bound, seeded RNG, sized to be
  deterministic-enough to gate CI).
- Benchmark: `candidate_construction`-style A/B at 5k-10k nodes,
  sampling on vs off; packing-quality table recorded in
  `benchmarks/scheduler/README.md`.

## Rollout

Opt-in only; absent knob = today's behavior, bit for bit. Documented in
`docs/scheduler-and-eval-loop.md` (surface, defaults, quality trade,
when to reach for it). Recorded quality numbers ship with the feature.
