# AWS phase 4 — the rig was measuring itself

**Date:** 2026-08-05. **Branch:** `aws-day-3`. **Status:** the throughput
numbers from phases 2 and 3 are retracted; the latency and CPU numbers stand.

## The finding

Every `web_fleet` throughput number this project has produced is the benchmark
driver's submission ceiling, not Legion's scheduling rate.

The driver submits one `legion job run` **CLI process per job** from a thread
pool of 4, on an `m7i.large` — 2 vCPU, shared with Prometheus and Grafana. That
capped submission at ~36 jobs/sec. When the scheduler is faster than that, the
cell reports the driver's rate and nothing says so.

| cell | jobs | submit span | submit rate | × allocs/job | reported throughput |
| --- | --- | --- | --- | --- | --- |
| A | 4,167 | 114.97 s | 36.24/s | 6.0 → 217 | **203.19** |
| B | 4,167 | 136.37 s | 30.56/s | 6.0 → 183 | **164.72** |

The confirming detail is the drain: the last allocation landed **0.169 s** after
the last submit in cell A and **0.097 s** in cell B. Legion was never behind, at
any point, in either cell. The control plane sat at **8% CPU of four cores**
with an **empty eval queue**.

`25,002 / 4,167 = 6.0` allocations per job exactly, and `36.24 × 6 ≈ 217`. The
reported throughput is arithmetically the submission rate.

### What this explains

Every anomaly phases 2–4 chased, at once:

- 8% CPU on the control plane, and no thread over 40%.
- `eval_queue_depth` at 0 for the 16-thread cells — the workers were starved.
- Throughput *falling* from 4 → 16 eval threads (more workers dividing a fixed
  arrival rate, each paying a wasted claim).
- The group-commit coalescer never batching (1.07 at 4 threads, 1.17 at 16):
  submitters cannot overlap when work arrives one job at a time.
- The wake-herd fix showing no throughput gain — it cannot, on a starved rig.

### What survives

The **2.53× is not entirely false, but its ratio is meaningless.** The 80.35
baseline *was* a real scheduler measurement: eval queue depth peaked at 576 and
p99 placement latency was 103.4 s, so the scheduler was genuinely behind. The
after number was a measurement of the load generator. What the phase-3 work
supports:

- p99 placement latency 103.4 s → **17.8 s**, p50 49.4 s → 11.9 s.
- 25,002 / 25,002 placed, 0 rejections.
- The scheduler stopped being the bottleneck at this arrival rate.

**Legion's actual maximum throughput is unmeasured.** 203.19 alloc/sec is a
floor, not a ceiling.

## Why it went unnoticed for three sessions

The knowledge was in the repo the whole time. `driver.py`'s own submit loop
carries the warning:

> Submission has to outpace placement or the driver, not the system under
> test, sets the measured rate.

and `dense_fleet.json`, written earlier in this same session, says:

> the driver — a 2-vCPU loadgen box spawning one CLI process per submit —
> reaches its ceiling long before the control plane reaches its own.

Both were written and then not applied to the numbers coming out the other end.
The rig had no check that turned that knowledge into a failure, so it reported
driver-bound cells as results, exactly as it once
[reported cells that measured nothing](aws-phase3-findings.md).

## Fixes

**Detection (`c0d9a44`).** The driver compares the drain after the last submit
against the measured window and emits
`submission{jobs, span_secs, rate_per_sec, drain_secs, driver_bound}`, warning
loudly when it is the constraint. Validated against both archived cells: both
flag `driver_bound: true`.

**Ceiling.** `loadgen_type` `m7i.large` → `m7i.4xlarge` (16 vCPU);
`--submit-concurrency` 4 → 32, settable per cell through `run-campaign.sh`.

**Cost per submit (`17564bd`).** The rig emitted one `.ncl` per Legion job, so
every submit evaluated Nickel inside the CLI for a spec that never changes.
Measured, 20 runs of `job validate` on one spec: **39 ms for `.ncl` against
10.75 ms for `.json`** — rendering was ~28 ms of every submit. It was also
unfair, since the Nomad and K8s adapters already submit pre-rendered artifacts,
so only Legion paid rendering inside the measured path. Now JSON by default,
with `--jobspec-format ncl` keeping the path a real user types.

## Scheduler work done anyway, and what is known about it

Both are correct on their merits and both are **unmeasured on AWS**.

**Wake herd (`8019227`).** `project_changeset_side_effects` called
`notify_waiters()` on *any* `eval` row change, waking every parked worker. Each
answers with `claim_eval`, a scheduler-lane quorum round whether or not it finds
work, and every eval crosses that path three times (enqueued, claimed,
completed). Measured cost: **16,678 scheduler-lane raft ops at 4 threads against
47,096 at 16**, for identical work. Now wakes one worker per newly-claimable
eval. A capture-level test pins it at 3 wakes before, 1 after.

**Async status projection (`e357f58`).** Takes the `task_status` projection off
the plan-apply path.

## Instrumentation added

- `legion_eval_claim_total{outcome}` — an empty claim costs a full quorum round
  and was invisible. Its absence is what hid the herd.
- `legion_raft_ops_total` gained an **`op`** label. `collect.sh` had been
  grouping `by (lane, op)` against a metric with no such label and silently
  getting lane totals back, which is why the status lane was unattributable.

## Open, in priority order

1. **Re-measure on an unstarved rig.** Nothing about Legion's throughput is
   known until a cell reports `driver_bound: false`.
2. **The status lane is the biggest raft consumer in the system.** Cell A:
   status 50,567 ops, control 25,838, scheduler 16,678. With 25,002 allocations
   at ~2 state transitions each, that is ~1 quorum round per allocation state
   change — more than scheduling itself. Heartbeats are already off raft (the
   local liveness tracker), so this is allocation status. Confirm with the new
   `op` label before touching it.
3. **A real bulk-submit path.** One CLI process per job is a poor way to drive
   load even at 16 vCPU. A long-lived submitter speaking gRPC directly would
   remove the rig's ceiling rather than raise it. This is a benchmark-harness
   change, not a product one — `job run` taking multiple paths would be a
   user-facing CLI surface and needs a plan.
