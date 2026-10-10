# AWS phase 2: what we found (2026-08-04)

The second night on the comparative-benchmark rig, and the first with
liveness off the raft path. It produced no publishable benchmark — that
was not the goal — but it found the scheduler's actual scaling limit, and
the limit is not what anyone expected.

Read this to pick the work back up. The plans it points at own the
decisions; this is the record of how we got to them.

- Plan: [`plans/aws-comparative-benchmark-plan.md`](plans/aws-comparative-benchmark-plan.md)
- Calls: [`calls/aws-comparative-benchmark-plan_calls.md`](calls/aws-comparative-benchmark-plan_calls.md) (entries 25-26 and 29-31; the load-generator fix that followed is 27-28)
- The night before: [`aws-phase1-findings.md`](aws-phase1-findings.md)
- Open work: [`TODO.md`](TODO.md)

## Status

**All AWS resources destroyed.** Rig (23 terraform resources) and the
operator box, its Elastic IP, security group, key pair and IAM role.
Only `s3://legion-bench-archive-594906412132/` remains, deliberately —
it is outside terraform. Local copies are in
`benchmarks/aws/results/2026-08-04-night/` (90 MB).

Spend for the night: **roughly $4**.

**Every cell placed 100% of its allocations.** Nothing here is a
correctness finding. It is entirely about throughput and latency.

## The headline: plan-apply conflicts, not capacity

Legion's late-binding plan apply is optimistic. Concurrent eval workers
plan against the same candidate nodes, and the loser re-plans. The
conflict rate is not a constant — it rises with both concurrency and
cluster size, and it rises fast:

| condition | rejection rate |
| --- | --- |
| 4 eval threads, 1000 nodes | 15.9% |
| 4 eval threads, through 5000 nodes | 31.9% |
| 16 eval threads, through 5000 nodes | **73.6%** |

Cumulative for the night: `plan_apply_total{applied}` 4974 against
`{rejected}` 14382 — **74.3%**. Roughly three quarters of all planning
work was computed and thrown away.

The rejection message is the tell:

> `no candidate for task N could bind: all 1 candidate(s) were stale or invalid`

**One candidate.** Meanwhile `legion_scheduler_index_candidates{index="fit"}`
shows the scheduling index handing placement 33-64 candidates per task.
The index is not candidate-starved; the submitted plan carries a single
candidate per task and loses it to whoever committed first.

### What it does to throughput

Web-fleet workload, ~5 allocations per node, `dist` binary, 3x
`m6id.xlarge` (4 vCPU) with the raft store on instance NVMe, lanes
topology, 3 voters:

| nodes | allocations | evalThreads | throughput | p50 | p99 |
| --- | --- | --- | --- | --- | --- |
| 1,000 | 5,004 | 4 | 354.6/s | 2.3 s | 3.8 s |
| 2,000 | 10,008 | 4 | 279.0/s | 5.7 s | 12.4 s |
| 5,000 | 25,002 | 4 | 86.6/s | 57.6 s | 177 s |
| 1,000 | 5,004 | 16 | 310.4/s | — | — |
| 5,000 | 25,002 | 16 | 77.3/s | 45.2 s | 141 s |

**These are bin_pack numbers.** The rig's
`configs/legion/manager-a.ncl.tpl` sets `schedulingPolicy = "bin_pack"`
while `web_fleet.json` declares `"scheduling_policy": "spread"`, and the
manager setting wins. That is not incidental to the finding: bin_pack is
the maximally correlated policy — every planner's argmax is the same
fullest-node-that-still-fits — so it is the worst case for optimistic
concurrency. The conflict rate under `spread` is unmeasured, and the two
configs should be made to agree before either is quoted.

Two things to take from this table.

**Throughput collapses rather than plateaus.** A saturated but healthy
system holds throughput flat and grows latency linearly. From 1k to 5k
nodes, throughput fell 4x while latency rose 25x. Something gets *worse*
with scale, not merely busier.

**Adding eval threads makes it worse, at both scales.** 4 -> 16 threads
cost 12% throughput at 1k and 11% at 5k, while box CPU went from ~29% to
~85%. The extra CPU went into plans that were discarded. This is the
single most useful result of the night, because it inverts the obvious
tuning advice.

## Legion is latency-bound, not CPU-bound

Worth stating plainly because it was measured wrong twice before it was
measured right.

At `evalThreads = 4` under a 5000-node cell:

- box CPU **29%**, 71% idle
- no single thread above **40%** of one core
- `legion_runtime_spawn_delay_seconds` entirely under **50us** — tasks
  never wait to get onto a thread

So the manager is not short of CPU, and no single thread is pegged
either. The waiting shows up here instead:

| lane | pipeline wait/op | propose_apply/op | ops |
| --- | --- | --- | --- |
| scheduler | **3.07 ms** | 1.51 ms | 67,165 |
| control | 0.75 ms | 1.87 ms | 37,216 |
| status | 0.09 ms | 1.51 ms | 66,622 |

`propose_apply` is flat at ~1.5ms on every lane, so replication itself is
cheap and uniform. The scheduler lane's pipeline wait is **34x** the
status lane's on comparable op volume — queueing behind that lane's
single `pipeline_loop` thread.

**And each eval needs about six trips through it.** 67,165 scheduler-lane
ops against ~10,700 evals: `claim_eval`, the plan apply,
`complete_eval_with_event`, plus enqueues, each its own replicated
proposal. At ~3ms of wait apiece that is ~18ms of queueing per eval
against 9.32ms of actual scheduling work — which is why the phase
histogram looks fast while throughput does not.

**A bigger control-plane instance will not help.** Fewer round trips will.

## Phase timings, and where the cost moved

Per-eval means, 1000 nodes only, then cumulative through 5000:

| phase | 1k | through 5k | growth |
| --- | --- | --- | --- |
| task_fetch | 0.29 ms | 9.64 ms | **33x** |
| build_plan | 1.72 ms | 7.01 ms | 4x |
| apply | 6.74 ms | 19.8 ms | 2.9x |
| capacity_snapshot | 0.23 ms | 0.85 ms | 3.7x |
| available_nodes | 0.03 ms | 0.12 ms | 4x |
| total | 9.32 ms | 46.4 ms | 5x |

`available_nodes` at 0.03ms and the index's bounded 33-64 candidates say
**candidate selection scales well with fleet size** — the part everyone
expects to be the problem is not.

`task_fetch` growing 33x is unexplained and is the top open question. Its
queries are all scoped by `job_config_id` and the indexes exist
(`idx_task_status_job_config_id`, `idx_task_config_job_config_id`,
`idx_allocation_job_config_id`), and SQLite is in WAL mode so readers are
not blocked by writers. The remaining hypothesis is contention on the
read-model lock (`read_model_reader()`, taken more than once per eval in
`task_fetch` alone) as the raft apply path writes into it harder at
scale. **Not verified.** In a later run at 16 threads `task_fetch` was
only 0.92ms, so the effect is load-shaped rather than purely size-shaped,
which the lock hypothesis would predict but does not prove.

## Everything else the night turned up

Each of these has a TODO entry with the evidence.

**Legion, performance**
- Mass node loss starves every other eval kind. `claim_eval` orders by
  `priority DESC, id ASC` and `node_lost_reschedule` shares the default
  priority 50 with normal jobs, so at equal priority the autoincrement id
  decides and a submit can never overtake a backlog. Measured: 5760
  reschedules against 2503 submits, a 764-deep queue, ~6 second submits.
  Not rig-only: a single-AZ loss in a three-AZ cluster is ~33% of the
  fleet, *under* `LIVENESS_MASS_DISRUPTION_FRACTION` (0.5), so the storm
  fires unsuppressed.
- Lost nodes with **zero allocations** still cost a full enqueue ->
  claim -> no-op -> complete cycle; the emptiness check is inside the
  handler, after two raft round trips.
- No batch job-submission RPC: a client submitting N jobs pays N round
  trips and N control-lane commits (~25ms per job end to end, of which
  ~2ms CLI startup, ~13ms Nickel evaluation, ~10ms commit).

**Legion, observability and reporting**
- ~~264 evals ended `status: failed` while
  `evals_processed_total{status="error"}` never moved.~~ **Retracted.**
  `src/manager/daemon.rs:2267` increments that counter on both the ok and
  error paths, and re-reading the transcript the "unchanged" value came
  from a metrics scrape taken *before* those failures, compared against
  an eval-table query taken after. Same mistake phase 1 warned about:
  two numbers, sampled at different times, read as if simultaneous. Not
  re-verifiable — the rig is gone. Worth one deliberate check next run.
- `legion_eval_queue_depth` is a single unlabelled gauge, so a 764-deep
  queue says nothing about what is in it.
- `legion job info` reports `status: done` for a job every one of whose
  tasks failed to place. `done` is simply the initial value written at
  insert, not a verdict.
- `DestroyJob` returns `{"code": "Internal", "message": "Error deleting
  job"}` when the real reason — "Job must be stopped before it can be
  destroyed" — is known server-side.
- No `legion_nodes_recovered_total` to pair with `nodes_marked_lost_total`,
  and re-registration clears a `lost` row with no log and no counter, so
  a fleet that dies and returns reads as though nothing happened.

**Legion, product gaps**
- **There is no way to remove a node record.** `Store::remove_node` is
  implemented, cross-lane correct and unit-tested, with no production
  caller, no RPC and no CLI verb. An operator decommissioning hardware
  has the same problem the rig does.

**The rig** — see the calls doc entries 25-26 and 29-31 for the reasoning
- Cells were sized from each scenario's own base (180/304/380
  allocations), so a 1000-node cell placed 180 allocations in 0.8s,
  entirely inside the warmup, and reported `throughput 0.0` while
  `complete: true`.
- The driver submitted serially at ~40 jobs/sec, and Legion placed every
  job before the next arrived — the last placement landed **0.0s** after
  the last submit. The cell measured Python's subprocess loop.
- Per-cell job teardown ran only after *successful* cells, because the
  driver exits non-zero on an incomplete one and `pipefail` aborted the
  script first. Later cells then resubmitted identical specs for jobs
  that still existed; Legion correctly treated that as a no-op and
  emitted no placement events, which looked exactly like a broken event
  stream.
- The fleet was shaped from web_fleet's `node_profiles` and reused for
  every scenario, so `batch_burst`'s `class: batch` constraints matched
  nothing and eight of twelve cells would have placed nothing.
- The fleet-size gate was `registered >= NODES` over a row count that
  ignores health — it passed with 4000 rows for a 1000-node cell.
- `teardown.sh` prompts for confirmation, and under `nohup < /dev/null`
  `read` hits EOF and `set -e` kills the script *before* it prints
  "aborted" — so an unattended teardown leaves the entire rig billing
  behind a log that ends on a successful-looking sync line.

## What we got wrong

1. **"The event watcher died."** Concluded from `pgrep` showing it absent
   *after* the run had ended, which proves nothing. The actual cause was
   the teardown never running, so the next cell resubmitted existing
   jobs. Two runs were spent on the wrong suspect.
2. **"Phase 1's smoke cell was contaminated by orphaned nodes."** The
   structural hole is real, but the archives disprove it for that night:
   every packing-density ladder broke at or below 1000 nodes, so the
   1000-node smoke cell was a superset and reused every stale name.
3. **"The single pipeline thread is pegged at one core."** Plausible, and
   it would have read as ~25% on a 4-vCPU box — but per-thread sampling
   showed the busiest thread at 40%. The serialization is real; the
   saturation is not.
4. **Concurrency 16 without checking the box.** The loadgen instance is
   an `m7i.large` — 2 vCPU. Sixteen concurrent CLI processes on two
   cores.

The through-line from phase 1 holds: every wrong turn came from reading
one number without the one next to it.

## Where to pick up

1. **Fix the conflict rate.** Everything else is second order while three
   quarters of planning work is discarded. Striped disjoint candidate
   lists are already Phase 4 of
   [`plans/late-binding-plan-apply-plan.md`](plans/late-binding-plan-apply-plan.md),
   and that phase was explicitly gated on the
   `plan_apply_total{rejected}` metric — which now has numbers.
2. **Cut the raft round trips per eval**, or widen the lane pipeline.
   ~6 replicated proposals per eval is the second-order cost, and it is
   what makes each retry expensive.
3. **Explain `task_fetch`'s 33x.** Verify or kill the read-model lock
   hypothesis before designing anything.
4. **Measure `spread` as well as `bin_pack`.** Everything above is
   bin_pack, which is the maximally correlated policy and therefore the
   worst case for optimistic concurrency: every planner's argmax is the
   same fullest-node-that-still-fits. `spread` should conflict far less,
   and the gap between the two is the clearest evidence of whether the
   conflict is inherent or policy-shaped. It also matters for the fix —
   the two-pass bind is a placement-quality *improvement* under bin_pack
   (stacking is what bin_pack wants) but a mild policy violation under
   spread, so spread is where its regression risk lives.
4. **Then re-measure.** The rig can reproduce all five rows of the table
   above; they are the regression test for every change here.
