# Next Up: Current Focus

**The scheduler's limit is plan-apply conflicts, and we now know it.**
Second AWS night, 2026-08-04
([findings](aws-phase2-findings.md)) — the campaign produced no
publishable benchmark, which was not its purpose. It produced a
diagnosis.

Three quarters of all planning work is computed and thrown away
(`plan_apply_total` 4974 applied against 14382 rejected), and the
rejection rate climbs with both concurrency and cluster size: 15.9% at 4
eval threads / 1000 nodes, 31.9% through 5000, **73.6%** at 16 threads.
Throughput inverts as a result — 4→16 eval threads *cost* 12% throughput
while CPU went 29%→85% — and collapses 4x from 1k to 5k nodes while p99
goes 3.8s → 177s.

The cause is specific. `decouple_plan_components`
(`src/manager/scheduler.rs:2158`) collapses every multi-placement plan to
a **single candidate per placement**, so late binding is structurally off
for exactly the workloads it was built for. Then the apply walk rejects
on a node-epoch mismatch before re-deriving fit — even though the same
transaction already re-derives capacity, constraints, ports and distinct
keys against committed state. "Stale" means only "the epoch moved", never
"it no longer fits".

Two design agents reached that independently, and a two-parameter model
of it reproduces all three measured rejection rates.

## In flight right now

1. **Plan-apply conflict reduction** —
   [plan](plans/plan-apply-conflict-reduction-plan.md). Two-pass bind
   (epoch becomes a ranking hint, not a gate), candidate lists restored,
   decorrelated tail sampling. Target: rejection under 5%, and throughput
   that *rises* with eval threads instead of falling.
2. **Adaptive scheduler concurrency** —
   [plan](plans/adaptive-scheduler-concurrency-plan.md), `PLANNED`.
   Congestion control for scheduler parallelism, so an operator never has
   to guess `evalThreads`. Deliberately gated behind (1): its Phase 0 is a
   measurement that decides whether the rest gets built at all.
3. **Eval-path raft cost.** ~6 replicated scheduler-lane ops per eval,
   and the lane's single `pipeline_loop` thread was ~100% busy
   (67,165 ops x ~3ms ≈ the 206s of measured `pipeline_wait`). Claim
   fuses with complete; enqueues batch; allocation removal folds into the
   apply. Target ≤ 2.5 ops/eval.
4. **Node-loss storm.** 5760 reschedule evals against 2503 submits, and a
   submit could not overtake the backlog because both sit at default
   priority 50 with a FIFO tie-break. Also: lost nodes with *zero*
   allocations still cost a full enqueue→claim→no-op→complete cycle, and
   `reschedule_allocations_for_lost_node` reconciles once per lost node
   per job config — O(lost x configs). Not rig-only: an AZ loss is ~33%
   of a fleet, under the mass-disruption breaker, so the storm fires
   unsuppressed.
5. **Diagnosis tooling and the operator runbook** —
   [plan](plans/scheduler-diagnosis-tooling-plan.md). Tonight's diagnosis
   took hours because the signals were scattered and several lied. A
   `legion scheduler diagnose` plus symptom-first guidance, so the next
   person starts from "throughput dropped when I scaled up" rather than
   from a metric name.
6. **The rig itself** — every cell of the first campaign was invalid for
   a different reason. All fixed; see the findings doc and calls entries
   21-25.

## Known open questions

- **`task_fetch` grew 33x** (0.29ms → 9.64ms per eval, 1k → 5k nodes)
  and nothing explains it yet. Its queries are all scoped by
  `job_config_id`, the indexes exist, and SQLite is in WAL mode. The
  standing hypothesis is contention on the read-model lock, taken more
  than once per eval. **Unverified** — and in a later 16-thread run it
  was only 0.92ms, so the effect is load-shaped rather than size-shaped.
- **Does `build_plan` stay flat at 10k+?** It was 1.72ms at 1k, and
  `available_nodes` costs 0.03ms with the index handing over a bounded
  33-64 candidates regardless of fleet size. That *suggests* placement
  cost is independent of fleet size, which is the whole scale claim — but
  it has never been measured above 5k.
- **Is there a node-deregistration story?** `Store::remove_node` is
  implemented, correct and tested, with no RPC, no CLI verb and no
  caller. Operators decommissioning hardware hit the same wall the rig
  did.

## Before the next campaign

The rig is ready (fleet shaped per scenario, exact-census gate, per-cell
teardown, cadence evidence per cell, allocation sweeps in one command).
The binary is not, until (1) lands. Re-run the five-row table in the
findings doc first — it is the regression test for every change above.

Higher-level backlog: [TODO.md](TODO.md).
