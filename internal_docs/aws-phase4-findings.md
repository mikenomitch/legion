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

## The first unstarved measurement

Cell G, 2026-08-05 14:05, `web_fleet` 5,000 nodes, `evalThreads=4`, on a
16-vCPU loadgen with concurrency 32 and pre-rendered JSON:

| metric | value |
| --- | --- |
| submission rate | **89.62 jobs/sec** (was 36.24) |
| drain after last submit | **101.05 s** |
| `driver_bound` | **false** |
| throughput | **177.27 alloc/sec** (~29.5 evals/sec) |
| p50 / p99 placement | 63.4 s / 100.8 s |
| placed | 25,002 / 25,002 |

Submission demands 538 alloc/sec and Legion delivers 177, so it is ~3x behind
with a 101-second backlog. This is real saturation.

Note the driver-bound figure (203.19) was *higher* than the true one. A slow
driver paces work so the scheduler never falls behind, which flatters both
throughput and latency.

## What the raft lanes are actually doing

Measured live during cell G with the new `op` label — the first time any of
this was attributable:

| lane | op | rate |
| --- | --- | --- |
| status | **`update_node_health_checks`** | **178.7/s** |
| scheduler | `complete_and_claim_next` | 26.6/s |
| status | `update_task_statuses_for_allocations` | 25.3/s |
| scheduler | `submit_and_apply_plan_with_candidates` | 25.3/s |
| status | `seed_task_statuses` | 21.1/s |
| control | `handle_job_submit` | 21.1/s |
| control | `idempotency_record` | 21.1/s |
| control | `replace_job_kv_references` | 21.1/s |
| control | `clear_job_failed_status` | 21.1/s |
| control | `clear_job_stopped_status` | 21.1/s |
| scheduler | `enqueue_eval_after_delay` | 21.1/s |
| scheduler | `sync_task_volume_claims` | 21.1/s |

Two findings, and the first corrects a guess made earlier in this document's
own history:

1. **The status lane is not allocation state transitions.**
   `update_task_statuses_for_allocations` is already batched at one round per
   eval (25.3/s), not one per allocation. The dominator is
   `update_node_health_checks` at **178.7/s** — 7x the eval rate — and it
   scales with FLEET SIZE, not workload. Heartbeats were moved off raft to the
   local liveness tracker; health checks were not.

2. **Job submit costs five separate control-lane quorum rounds.**
   `handle_job_submit`, `idempotency_record`, `replace_job_kv_references`,
   `clear_job_failed_status` and `clear_job_stopped_status` all run at exactly
   the submission rate. They are one logical operation and are candidates to
   fuse into one round, the same way `complete_and_claim_next` fused the eval
   queue's pair.

## The wake herd, measured

Cells G and H are the same binary on the same rig, differing only in
`evalThreads`, and both report `driver_bound: false`:

| cell | evalThreads | throughput | p50 | p99 | submit rate | drain |
| --- | --- | --- | --- | --- | --- | --- |
| G | 4 | 177.27 alloc/s | 63.4 s | 100.8 s | 89.62/s | 101.05 s |
| **H** | **16** | **219.25 alloc/s** | 54.1 s | 67.3 s | 69.21/s | 56.97 s |

**+23.7% throughput from concurrency, and p99 down 33%.** Before the fix the
same comparison ran the other way (203.19 at 4 threads against 164.72 at 16).
The sign of the relationship flipped, which is the result the fix predicted and
the one a starved rig could never have shown.

`legion_eval_claim_total` during cell H: **0.3 empty claims/sec** on the
scheduler lane, against a herd that used to put 30,000 extra ops on that lane
per cell.

## What the raft ops cost, by outcome

The op label alone is misleading — `Store::replicated` skips the raft entry
entirely when a mutation's changeset is empty, so a high op rate can be free.
Split by outcome, during cell H:

| lane | op | outcome | rate |
| --- | --- | --- | --- |
| status | `update_node_health_checks` | **empty** | 133.9/s |
| scheduler | `complete_and_claim_next` | applied | 44/s |
| status | `update_task_statuses_for_allocations` | applied | 16.2/s |
| scheduler | `submit_and_apply_plans` | applied | 11.2/s |
| scheduler | `submit_and_apply_plan_with_candidates` | applied | 9.3/s |
| scheduler | `claim_eval` | **empty** | 0.3/s |
| control | `clear_job_failed_status` | **empty** | 0/s |

This retires two targets this document previously named:

- **`update_node_health_checks` is not a bottleneck.** A local probe
  (`capture_mutation` on an unchanged versus changed value) returns a
  **0-byte** changeset when the health check is unchanged and 65 bytes when it
  is not, so these never reach quorum. The residual cost is the capture itself
  — a savepoint and a write-connection acquisition ~134 times a second — which
  is real lock contention but not a raft round.
- **The five control-lane rounds per job submit are mostly empty too**
  (`clear_job_failed_status` at `empty`), so fusing them buys much less than
  their op rate suggested.

The genuine quorum cost is the eval path: `complete_and_claim_next` at 44/s
plus ~20/s of plan applies.

## The real ceiling at scale: the plan-apply commit is serialized

The `dense_fleet` ladder found a cliff, and chasing it corrected a wrong
diagnosis of my own.

| allocations | throughput | drain |
| --- | --- | --- |
| 50,000 | 736.1 alloc/s | 58.0 s |
| 100,000 | 456.58 alloc/s | 177.8 s |

Plan phases at 100,000 against 25,000: `apply` 20.96 ms -> **354 ms**,
`build_plan` 9.10 -> 20 ms, `capacity_snapshot` 1.40 -> 4 ms, total 36.86 ->
381 ms. `apply` is 93% of plan time.

The first guess was that SQLite was thrashing its 2 MiB default page cache.
That was wrong, and the timer boundary is why: `apply_us` wraps the whole
`replicated()` call, so it contains the coalescer wait, the raft propose, the
quorum round trip and the post-commit cache refresh — it is not a
SQLite-read timer. Measured during the 200,000 cell:

| metric | value |
| --- | --- |
| coalescer **follower wait** | **347 ms** |
| coalescer commit | 138 ms |
| coalescer leader wait | 66 ms |
| batch size | 1.32 |
| scheduler pipeline wait | 20 ms |

**The 354 ms is 347 ms of queueing.** Plan-apply holds a single commit lock,
one commit costs 138 ms at this scale, and 16 eval threads pile up behind it.
Throughput follows directly: ~7 commits/sec x 44 allocations is the observed
rate. The page cache was raised anyway as a cheap hedge on the 138 ms, but it
is not the explanation and the commit message says so.

Note the batch size: **1.32**, with 16 threads contending. The coalescer is
being waited on without doing the batching that would justify the wait. A
submitter that has already lost the lock could join the in-flight batch
instead of forming the next one.

### Reads the scheduler should not be doing

The planner reads the in-memory `SchedulingReadModel` — that is why
`build_plan` stays at 20 ms. The apply path deliberately re-reads committed
state inside the transaction, and must: the fence cannot bind against a cache
refreshed only after commit.

The avoidable read is on the write-back side. After commit,
`cache_refresh_allocation_scopes` calls `allocs_for_node` for every touched
node and re-reads that node's ENTIRE allocation list, though the changeset
already names exactly which allocations changed. At ~40 allocations per node
across ~44 nodes that is ~1,700 rows re-read per eval to learn about 44
changes, on every replica.

## Open, in priority order

1. **Break the serialized plan-apply commit.** 347 ms of follower wait against
   a 138 ms commit is the ceiling at scale. Either make the commit cheaper or
   let submitters join an in-flight batch rather than queue behind it — the
   coalescer is charging for batching it is not doing (batch size 1.32 at 16
   threads).
2. **Apply a delta to the read model instead of re-reading each node.**
   ~1,700 rows re-read per eval, on every replica, to learn about 44 changes.
3. **Find the knee.** 16 threads beat 4; the ladder above 16 is unmeasured.
3. **Capture cost, not raft cost.** 134 empty captures/sec still take the write
   connection. Skipping the write when the value is unchanged would remove
   them entirely.
3. **A real bulk-submit path.** One CLI process per job is a poor way to drive
   load even at 16 vCPU. A long-lived submitter speaking gRPC directly would
   remove the rig's ceiling rather than raise it. This is a benchmark-harness
   change, not a product one — `job run` taking multiple paths would be a
   user-facing CLI surface and needs a plan.
