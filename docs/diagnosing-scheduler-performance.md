# Diagnosing Scheduler Performance

You are here because placements are slow, throughput dropped, or work is not
running and nothing obvious is broken. This page is organized by **symptom**,
not by metric. Find your row, read the decision, take the action.

Every worked example below is a real measurement from a Legion scale campaign,
including the ones where the first reading was wrong.

> **The one rule.** Never act on a single number. Every metric on this page has
> a **neighbour** that decides what it means. `plan_apply` latency without
> `pipeline_wait` next to it, or a slow placement rate without the freshness
> ratio next to it, will send you at the wrong thing. In the campaign this page
> comes from, every wrong turn came from reading one number and not the one next
> to it, and the wrong turns cost more than the bugs did.

---

## Start here: run the command

```console
$ legion scheduler diagnose
```

One read-only command that samples the manager's metrics over a window,
applies every check on this page, and prints a verdict with the remedy
attached. It exists because working through this page by hand takes an hour
and a Prometheus scrape, and because the rule above is easy to break at 3am.

```console
$ legion scheduler diagnose
Scheduler                 DEGRADED   (sampled 30s)

  ✗ plan_rejection_storm            critical
    31.9% of plans rejected (1,247 rejected / 3,912 total)
    Each rejection is a discarded plan plus a full eval retry: the work is
    done twice and placed once.
    rejectionShare=0.319; planApplyApplied=2503; planApplyRejected=1247
    Ruled out: candidate_lists_disabled (planCandidates=8); pool_full
               (candidateRankP50=0.0)
    → Reduce evalThreads from 16 to 8 and re-measure. Fewer planners racing
      means fewer discarded plans, and measured throughput goes up, not down.
      docs/diagnosing-scheduler-performance.md#symptom-throughput-fell-when-i-scaled-up

  ✓ heartbeat_ceiling               freshness 0.98 (false loss below 0.20)
  ✓ scheduler_lane_queued           scheduler lane wait 0.09ms against 0.08ms on status
  ✓ phase_growth                    apply 6.70ms of 9.30ms total
  ✓ eval_starvation                 job_submit 61.0% of processed evals
  ✓ invisible_eval_failures         0 failed evals in the window
  ✓ capacity_exhausted              184 of 200 available nodes still have room

Nothing here mutates the cluster. See docs/diagnosing-scheduler-performance.md.
```

Four things about that output are deliberate:

- **Checks that PASSED are printed, with their numbers.** "eval_starvation ✓
  job_submit 61%" is the reading that stops the next responder
  re-investigating starvation. Silence on a passing check reproduces the
  exact failure this page exists to fix.
- **`Ruled out:` is required on every failing check.** A check that cannot
  name what it excluded — and the number that excluded it — is a bug in the
  command, not a terse report.
- **The remedy names the field, the direction, and the current value.** It
  reads `evalThreads` out of the manager's own config, so "reduce evalThreads
  from 16 to 8" needs no second lookup.
- **The window is in the header.** Every rate is a delta over that window,
  never a since-boot average.

| Flag | Meaning |
| --- | --- |
| `--sample <dur>` | Sampling window, default `30s`. The command blocks for it. `0s` reports since-boot values and says so in the header. Maximum `300s`. |
| `--watch` | Re-sample and reprint until interrupted. |
| `-j` / `--json` | The standard envelope, for agents and scripts. |

### The finding codes

Stable, additive-only identifiers you can branch on in a runbook or an agent.
Each links to the section of this page that explains it.

| Code | Fires when | The discriminator it carries |
| --- | --- | --- |
| [`heartbeat_ceiling`](#symptom-slow-placements-and-nodes-flapping-lost) | Freshness is below 0.5, or below the false-loss line | Served interval, lost window, fleet size, lost-vs-recovered counts |
| [`plan_rejection_storm`](#symptom-throughput-fell-when-i-scaled-up) | Rejection share above 5% (warning) / 20% (critical) | `plan_candidates`, candidate-rank p50, index candidates per task, the rejection reason mix |
| [`scheduler_lane_queued`](#symptom-the-box-is-idle-and-throughput-is-still-capped) | Scheduler-lane `pipeline_wait` p99 above 1ms **and** 5x the quietest lane, with `propose_apply` flat | Spawn delay, in-flight vs worker threads, per-lane `propose_apply` |
| [`phase_growth`](#symptom-it-got-slower-as-the-cluster-grew) | One phase holds over half of `total`, or over 20% is outside any phase | The full phase mix as percentages |
| [`eval_starvation`](#symptom-new-work-waits-behind-a-backlog-of-other-work) | One kind is over 60% of processed evals **and** depth is over 100 | The full kind mix and the queue depth |
| [`invisible_eval_failures`](#symptom-nothing-is-running-and-nothing-reports-an-error) | Any eval failed terminally in the window | The failure count by kind, against `evals_processed_total{status}` |
| [`capacity_exhausted`](#symptom-the-cluster-is-full-and-no-knob-helps) | No available node has room while work is pending | Free capacity per pool |

`heartbeat_ceiling` is evaluated first and printed first whatever its
severity. When it fires, every other finding is marked **secondary**: a fleet
that cannot sustain its cadence generates scheduler load, and tuning the
scheduler first is the most expensive wrong turn on this page.

### If you would rather read the metrics yourself

The rest of this page is the manual version, and stays the authority on what
each signal means. Everything below is what the command automates.

---

## Start here

| What you are seeing | Go to |
| --- | --- |
| Throughput **fell** when I added eval threads, or when the cluster grew | [Rejection storm](#symptom-throughput-fell-when-i-scaled-up) |
| Throughput is capped but the box is **idle** | [Latency-bound, not CPU-bound](#symptom-the-box-is-idle-and-throughput-is-still-capped) |
| Everything got slower **in proportion to fleet size** | [Phase growth](#symptom-it-got-slower-as-the-cluster-grew) |
| New submissions take seconds while the cluster is otherwise busy | [Eval starvation](#symptom-new-work-waits-behind-a-backlog-of-other-work) |
| Placements look glacial and nodes keep going `lost` | [Heartbeat ceiling](#symptom-slow-placements-and-nodes-flapping-lost) |
| Nothing is running, and nothing reports an error | [Invisible failures](#symptom-nothing-is-running-and-nothing-reports-an-error) |
| Work will not place and the fleet looks full | [Capacity exhausted](#symptom-the-cluster-is-full-and-no-knob-helps) |

Before any of them, take **thirty seconds** on the two questions that decide
whether you are even in the right subsystem:

```console
$ legion node cadence          # is the control plane behind on liveness?
$ curl -s localhost:4749/metrics | grep -E 'legion_(runtime_spawn_delay|grpc_in_flight|runtime_worker_threads)'
```

- A **freshness ratio below 0.5** means you have a liveness problem that is
  *generating* scheduler load. Fix that first
  ([Heartbeat ceiling](#symptom-slow-placements-and-nodes-flapping-lost)).
- **`legion_runtime_spawn_delay_seconds` in the milliseconds** means the tokio
  runtime is saturated by blocking handlers and *every* RPC is slow, scheduler
  or not. That is the manager-wide problem, and
  [Metrics](metrics.md#diagnosing-a-manager-throughput-ceiling) owns its read
  order.

If both are clean, the problem is in the scheduler and the sections below apply.

### Reading counters correctly

Every `_total` family is **cumulative since process start**. A ratio computed
over the whole lifetime of a manager that has been up for six hours will not
show you a storm that started ten minutes ago. Take two samples and subtract:

```console
$ curl -s localhost:4749/metrics | grep plan_apply_total > /tmp/a
$ sleep 30
$ curl -s localhost:4749/metrics | grep plan_apply_total > /tmp/b
$ diff /tmp/a /tmp/b
```

The same applies to every histogram: `sum / count` since boot is an average
over the whole run, not the current latency. Subtract two samples of both.

---

## Symptom: throughput fell when I scaled up

**Decision: reduce concurrency.** Lower `evalThreads`. This is
counter-intuitive and it is correct.

**Read**

```
legion_plan_apply_total{outcome="rejected"}
legion_plan_apply_total{outcome="applied"}
legion_plan_apply_total{outcome="fallback"}
legion_plan_rejected_total{reason}          # and WHY they were rejected
```

Compute the rejection share over a window (not since boot):
`rejected / (applied + fallback + rejected)`.

| Rejection share | Meaning |
| --- | --- |
| under 5% | Normal contention. Ignore it. |
| 5–20% | Every rejection is a wasted plan **plus** a full eval retry. Worth acting on. |
| over 20% | Collapse. You are spending more work on losing races than on placing. |

**The neighbour that rules things in and out**

- `legion_plan_apply_total{outcome="fallback"}` **high, `rejected` low** means
  late-binding candidate lists are absorbing the races. Working as designed, do
  nothing. See [Late-binding plan apply](late-binding-plan-apply.md).
- `legion_scheduler_plan_candidate_rank` showing **deep ranks** alongside high
  `rejected` means the pool is effectively full. Add capacity; no scheduler knob
  will fix a full pool.
- `legion_scheduler_index_candidates{index}` showing **plenty of candidates**
  while rejections climb means the plan is not candidate-starved. It is losing
  its candidates to a concurrent apply, which is the case this section is about.
- CPU **rising** while placements **fall** is the signature. Contention burns
  CPU on work that gets thrown away.

**Worked example**

One campaign, three cells, same code:

| Eval threads | Nodes | `rejected` share | Placements | Manager CPU |
| ---: | ---: | ---: | ---: | ---: |
| 4 | 1,000 | 15.9% | 354.6 alloc/sec | 29% |
| 4 | 5,000 | 31.9% | — | — |
| 16 | 5,000 | 73.6% | 310.4 alloc/sec | 85% |

Four times the eval threads bought **12% fewer placements** and **three times
the CPU**. At 73.6%, three of every four plans built were discarded and their
evals retried. The rejection messages read "no candidate for task N could bind:
all 1 candidate(s) were stale or invalid" while the scheduling index was handing
placement 33–64 candidates per task, which is what proves the plans were losing
races rather than running out of nodes.

Those numbers predate the two-pass bind
([late-binding-plan-apply.md](late-binding-plan-apply.md)), which removed the
mechanism behind them: a candidate whose node epoch moved is now revalidated
against committed state and bound if it still fits, so contention no longer
converts into rejections. If you are on a build that has it, check
`legion_scheduler_plan_bind_pass{pass="revalidated"}` before reaching for
`evalThreads` — a large `revalidated` count with a flat `rejected` counter means
the contention is being absorbed and the remaining rejections are real capacity.

**Actions, in order**

1. **Lower `evalThreads`** in the manager config and re-measure. On the numbers
   above, 4 beat 16 outright. Start at half and bisect.

   ```nickel
   # configs/manager.ncl
   { evalThreads = 4 }
   ```

   The default is `available_parallelism()`, which on a large control-plane box
   is very likely too high for a contended workload. There is no relationship
   between core count and the concurrency the optimistic-concurrency fence can
   absorb.

2. **Confirm candidate lists are on.** `schedulerSettings.plan_candidates`
   defaults to `8`; `0` disables them and restores single-node submissions,
   which turns every race into a rejection. If someone set it to `0` for a
   mixed-version upgrade window and never set it back, that alone can produce
   this symptom.

3. **Do not raise `plan_conflict_max_retries` first.** More retries means more
   plans built against a snapshot that is already losing. It changes where the
   work is wasted, not how much.

### Why the plan was rejected

The rejection *share* alone cannot tell contention from fleet churn, and they
take opposite remedies — which is the single most expensive guess available on
this page. `legion_plan_rejected_total{reason}` is the discriminator:

```
legion_plan_rejected_total{reason="node_epoch_moved"}     # contention
legion_plan_rejected_total{reason="node_unavailable"}     # fleet churn
```

The totals agree with `legion_plan_apply_total{outcome="rejected"}`; it is a
separate family so the existing one's series shape is unchanged.

| `reason` | Cause | Remedy |
| --- | --- | --- |
| `node_epoch_moved` | A concurrent apply took the node *and* the placement could not be revalidated onto it — only an edge-boot ratification (`target_instance`) rejects on epoch alone. | **The signature of too much concurrency.** Reduce `evalThreads`. If the volume is on `speculativeStart` jobs, look there instead. |
| `job_epoch_stale` | Two plans raced on one job config | Reduce concurrency; job-config epochs are a hard all-or-nothing fence. |
| `node_unavailable` | Cordoned, draining, or lost since the plan snapshot | **Fleet churn, not contention.** Reducing `evalThreads` will not help. Find what is cordoning or losing nodes. |
| `capacity_shrank` | A worker re-registered with smaller totals | Check worker configs and re-registrations. |
| `late_constraint_failure` | Constraints or permits no longer satisfied | Check node attribute churn. |
| `node_gone` | The node row vanished mid-plan | Usually deregistration or a wiped fleet. |
| `capacity_recheck_budget` | The plan ran out of its per-plan capacity-recheck budget | Contention plus long candidate lists; reduce concurrency. |
| `static_port_taken` | A static port collided at bind time | Two jobs want the same host port. A jobspec problem, not a tuning one. |
| `no_candidates` | The placement carried no candidate list | `schedulerSettings.plan_candidates` is `0` — the mixed-version kill switch. Set it back to `8`. |

`legion scheduler diagnose` branches on exactly this mix: a churn-dominated
report drops the `evalThreads` advice and tells you to investigate the fleet
instead.

The free-text `plan_submission.rejection_reason` column is still there and still
richer — it names the node and the epochs — when you need to look at individual
rejections rather than the mix:

```console
$ legion state query "SELECT rejection_reason FROM plan_submission \
    WHERE status = 'rejected' ORDER BY id DESC LIMIT 20;"
```

---

## Symptom: the box is idle and throughput is still capped

**Decision: do not resize the instance.** A bigger box will not help. You are
queued, not busy.

**Read**

```
legion_raft_pipeline_wait_seconds{lane="scheduler"}
legion_raft_pipeline_wait_seconds{lane="status"}
legion_raft_propose_apply_seconds{lane}
legion_raft_pipeline_depth{lane}
```

Each raft lane is served by **exactly one OS thread** (capture, propose, and
apply are serialized per lane, see [Multi-raft](multi-raft.md)). Queueing in
front of that thread is a first-class latency component and it is invisible in
every other histogram, because those only start once a job is picked up.

**The neighbour that rules things in and out**

| If | Then |
| --- | --- |
| `pipeline_wait` grew, `propose_apply` flat | The lane's single pipeline thread is the constraint. Replication is fine. **More CPU will not help.** |
| `propose_apply` grew, `pipeline_wait` flat | Replication is the constraint: network, fsync, or a lagging follower. Faster disk or better-connected voters help. |
| `legion_raft_capture_seconds` grew | Local SQLite work grew. Look at read-model size and the apply phase. |
| None of the three moved | The time is outside the store. Check `legion_runtime_spawn_delay_seconds` next. |

**Worked example**

A control plane at **29% CPU**, with no thread above 40% of a core and
`legion_runtime_spawn_delay_seconds` entirely under 50µs, was capped on
placement throughput. The runtime was not saturated and no handler was slow. The
tell was one comparison:

| Lane | `pipeline_wait` per op | `propose_apply` |
| --- | ---: | ---: |
| scheduler | **3.07 ms** | ~1.5 ms |
| status | 0.09 ms | ~1.5 ms |

34x the queueing on the scheduler lane, at identical replication cost, on
comparable volume. In absolute terms the scheduler lane accumulated **206
seconds** of pipeline wait over the run. The cause was op count: 67,165 raft ops
for roughly 10,700 evals, about **six replicated proposals per eval**
(`claim_eval`, the plan apply, `complete_eval`, plus enqueues, each its own
proposal). Meanwhile `legion_scheduler_plan_phase_seconds{phase="total"}`
averaged 9.32ms per eval against an achieved wall time of roughly 75ms per eval
per thread. The scheduling work was not the problem. The queue in front of the
lane was.

**Actions**

1. **Reduce `evalThreads`.** More eval threads produce more proposals into the
   same single-threaded lane. This symptom and the
   [rejection storm](#symptom-throughput-fell-when-i-scaled-up) have the same
   remedy, for different reasons, which is why they so often appear together.
2. **Check topology.** Under `topology = "single"` every lane rides one raft
   group, so scheduler work queues behind status and control work. `lanes` gives
   the scheduler its own pipeline thread. See [Multi-raft](multi-raft.md).
3. **Check `legion_store_coalesce_batch_size{path="plan_apply"}`.** If the
   `le="1"` bucket is close to `_count`, group commit is not engaging and each
   plan is paying a full commit. Batching is opportunistic: it only forms from
   submitters that overlap, so "configured" and "happening" are different claims
   and only this histogram separates them.
4. **Do not** buy a bigger instance on this signature. Nothing in the picture is
   CPU-bound.

**Known limitation.** One pipeline thread per lane is architectural today.
Reducing proposals per eval and widening the pipeline are both tracked in
[internal_docs/TODO.md](../internal_docs/TODO.md).

---

## Symptom: it got slower as the cluster grew

**Decision: find the phase that grew, then act on that phase specifically.**
Growth is not uniform, and the phases have completely different remedies.

**Read**

```
legion_scheduler_plan_phase_seconds{phase}
```

Phases, in execution order: `available_nodes`, `version_sample`, `task_fetch`,
`capacity_snapshot`, `build_plan`, `apply`, `total`.

**Worked example**

The same workload as the cluster grew from 1,000 to 5,000 nodes:

| Phase | 1,000 nodes | 5,000 nodes | Growth |
| --- | ---: | ---: | ---: |
| `task_fetch` | 0.29 ms | 9.64 ms | **33x** |
| `build_plan` | 1.72 ms | 7.01 ms | 4.1x |
| `apply` | 6.74 ms | 19.8 ms | 2.9x |
| `available_nodes` | 0.03 ms | — | flat |

A 5x fleet produced a 33x `task_fetch`. That is superlinear and it is the
finding, not `apply` being the largest absolute number. Reading only the biggest
bar would have sent an investigation at the apply path, which grew the *least*
in relative terms.

**What each phase growing means**

| Phase | Growing means | Do |
| --- | --- | --- |
| `available_nodes` | The node snapshot is being rebuilt instead of shared by reference. Some mutation is invalidating the cached list every plan. | Look for constant node churn: registrations, cordons, availability flapping. |
| `version_sample` | The lazy epoch sample is falling back to the eager whole-fleet sample. | Check `legion_scheduler_lazy_sample_fallback_total`. A high rate means heavy concurrent churn; see [rejection storm](#symptom-throughput-fell-when-i-scaled-up). |
| `task_fetch` | Task-config loading is scaling with something it should not. Superlinear here is a bug signature, not a capacity signature. | Capture the numbers and file them. No config knob fixes this. |
| `capacity_snapshot` | Allocation count per plan is growing. Expected as the cluster fills. | Nothing to tune; it is real work. |
| `build_plan` | Candidate construction and scoring are growing with fleet size. **Expected**, and the one phase with a direct knob. | Set `schedulerSettings.score_sample` to bound scoring cost, and/or declare a [scheduling index](scheduling-indexes.md). |
| `apply` | The replicated write is growing. | Split it: `pipeline_wait` vs `propose_apply` vs `capture` (see [above](#symptom-the-box-is-idle-and-throughput-is-still-capped)). |
| `total` much larger than the sum of the phases | Time is being spent outside the instrumented phases. | Check `legion_runtime_spawn_delay_seconds` and lock waits. |

**Bounding `build_plan` directly**

```nickel
# configs/manager.ncl
{
  schedulerSettings = {
    # Below min_nodes eligible candidates, behavior is exactly the default
    # (exact scoring). At or above it, score `sample` uniformly-drawn
    # candidates and take the best, which keeps per-placement cost flat at
    # any fleet size.
    score_sample = { min_nodes = 1024, sample = 128 },
  },
}
```

Feasibility is never sampled: constraints, permits, distinct rules, and resource
fit always run in full, so a sampled placement is always valid. Only the score
*ranking* is approximate. See
[Scheduler and eval loop](scheduler-and-eval-loop.md#bounded-candidate-scoring-score_sample).

---

## Symptom: new work waits behind a backlog of other work

**Decision: this is queue ordering, not throughput.** Do not add eval threads.
Confirm the mix first.

**Read**

```
legion_evals_processed_total{kind}      # by kind, over a window
legion_eval_queue_depth{kind}           # what is queued, not just how much
```

**Worked example**

After a mass node-loss event:

| Eval kind | Processed |
| --- | ---: |
| `node_lost_reschedule` | 5,760 |
| `job_submit` | 2,503 |

with `legion_eval_queue_depth` at **764** and job submissions taking **6
seconds** each. The scheduler was not slow. It was busy with recovery work, and
submissions could not overtake it: `claim_eval` orders by `priority DESC, id
ASC`, and a `node_lost_reschedule` enqueued with no priority inherits the same
default (50) that a normal job gets. At equal priority the autoincrement id
decides, so a new submit can **never** overtake an older backlog.

**Reading the composition**

`legion_eval_queue_depth` carries a `{kind}` label, so the queue says what is
in it rather than only how much:

```
legion_eval_queue_depth{kind="node_lost_reschedule"}  700
legion_eval_queue_depth{kind="job_submit"}             64
```

`sum(legion_eval_queue_depth)` is the old unlabelled value, unchanged, so
existing dashboards keep working. A kind that drains reads `0` rather than
disappearing.

The gauge is maintained by the writes each manager performs, and the eval queue
is a replicated table — so on a manager that is not leading the scheduler lane
the gauge is stale by construction. `legion scheduler diagnose` reads the table
for this reason and reports the gauge beside it. By hand:

```console
$ legion state query "SELECT type AS kind, status, COUNT(*) FROM eval \
    WHERE status IN ('pending','in_flight') GROUP BY 1,2 ORDER BY 3 DESC;"
```

`legion state seed-queries` installs `agent:pending-evals` and
`agent:recent-failed-evals`, which cover the same ground as saved queries for
agents and the UI State Explorer.

**Known limitation.** There is no queue-*age* metric — the eval table has no
creation timestamp — so a 764-deep queue draining in two seconds and one taking
six seconds per item produce the same reading, and `eval_starvation` fires on
depth plus kind mix rather than on how long anything has actually waited.

**Actions**

- **Raise the priority of the work you care about**, per submission:
  `legion job run --priority N` or `--asap`. This is the only lever that works
  today. A job's [priority band](preemption-and-eviction.md) also sets the
  default queue priority for its evals.
- **Expect this after any large node event.** A single-AZ loss in a three-AZ
  cluster is about 33% of the fleet, which is *under* the 50% mass-disruption
  circuit breaker, so the full storm fires unsuppressed. See
  [Heartbeat cadence and scale](heartbeat-cadence-and-scale.md#safety-valves).
- **Do not add eval threads to drain the backlog.** The backlog is raft-op bound
  (see [above](#symptom-the-box-is-idle-and-throughput-is-still-capped)), and
  more threads raise the rejection rate.

Cross-kind fairness in `claim_eval`, and cheaper handling of lost nodes that had
no allocations, are tracked in
[internal_docs/TODO.md](../internal_docs/TODO.md).

---

## Symptom: slow placements and nodes flapping `lost`

**Decision: check the freshness ratio before you touch anything in the
scheduler.** A control plane behind on heartbeats presents as a slow scheduler,
and the remedy is in a different subsystem.

**Read**

```console
$ legion node cadence
Fleet             1000 nodes
Served interval   2s  (workers use their worker-type heartbeatSeconds)
Lost window       10s  (5 missed beats)
Freshness         0.22 ── FALLING BEHIND  (false loss below 0.20)
Sustainable       ~220 nodes at 2s on this control plane
Suggested         heartbeatSeconds: 19   (lost window 95s)
```

**`legion_heartbeat_freshness_ratio` is the discriminator.** False node loss
begins when it falls below `served_interval / lost_window`, which is **0.20** at
the defaults. Below that line, healthy nodes get marked lost, which generates
reschedule evals, which adds load, which lowers the ratio further. It is a
feedback loop, and from inside the scheduler metrics it looks like the scheduler
degrading.

**Worked example**

A campaign cell reported **0.9 placements/sec at a p50 of 109 seconds** and the
numbers were quoted as scheduler throughput. They were not. The fleet was
achieving a 0.22 freshness ratio against a 0.20 false-loss threshold, roughly
10% away from marking a healthy fleet dead. The measured heartbeat rate was
~80/sec against the ~500/sec a 1,000-node fleet at a 2s cadence requires, with
**nothing saturated**: manager CPU 19–23%, sim hosts at 0.2%, and
`RaftTransport/AppendEntries` answering in 3ms. Those placement figures measured
the heartbeat ceiling, not the scheduler, and are not usable as scheduler
numbers.

**The neighbour that rules things in and out**

- `legion_nodes_marked_lost_total` **rising** while the machines are demonstrably
  alive is false loss. Believe the control plane broke, not the fleet.
- `legion_liveness_loss_suppressed_total{reason}` non-zero means a safety valve
  fired: `mass_disruption` (over half the fleet would have been lost in one
  tick), `failover_grace` (a manager just took over liveness), or
  `peer_digest_gap`. "Nothing was marked lost" and "nothing was wrong" are
  different states and this counter separates them.
- `legion_liveness_digest_rpcs_total{outcome="unimplemented"}` non-zero means a
  manager that predates liveness digests leads the status lane and heartbeats
  have fallen back to replicated writes. Finish the upgrade roll.
- `legion_nodes{status="lost"}` is "nodes are away";
  `legion_nodes_marked_lost_total` is "nodes went away". You need the second one
  to see flapping — and its mirror,
  `legion_nodes_recovered_total{path}` (`heartbeat` or `registration`), to tell
  flapping from a fleet that genuinely shrank. `marked_lost` climbing with
  `recovered` climbing behind it is nodes bouncing, which is a control-plane
  cadence problem; `marked_lost` climbing alone is hardware. Both are zero on a
  cluster that has lost nothing, which is the correct reading and not a broken
  exporter.

**Actions**

1. **Take the number `legion node cadence` hands you.** It computes
   `heartbeatSeconds` for this fleet size on this control plane at the
   configured headroom. Set it on the worker type.
2. **Or turn on adaptive cadence** so it moves on its own:

   ```nickel
   # configs/manager.ncl
   heartbeat = {
     targetRatePerSecond = 80,   # measure yours; off by default
     minInterval = "2s",
     maxInterval = "120s",
     missedBeatsBeforeLost = 5,
     headroom = 2,
   }
   ```

   The lost window always moves with the cadence
   (`missedBeatsBeforeLost x interval`), so backing off does not destroy
   detection margin the way a fixed window did.
3. **Accept the cost.** Detection gets slower in exact proportion to the
   back-off: at 1,000 nodes and R=80, a genuinely dead machine takes up to 125
   seconds to notice instead of 10.

Full model, sizing formula, and measurement recipe:
[Heartbeat cadence and scale](heartbeat-cadence-and-scale.md).

**A storage note that saves an afternoon.** On identical code and topology, the
same control plane sustained ~86 heartbeat commits/sec on EBS gp3 and ~2,000 on
local NVMe. If your raft store is on network storage, that is the first thing to
change, and it is invisible in every scheduler metric.

---

## Symptom: nothing is running and nothing reports an error

**Decision: do not trust `legion job info`'s status field.** It is a lifecycle
flag, not a placement verdict. Read
`legion_evals_failed_total{kind,reason}`, or count allocations.

**Worked example**

A run ended with **264 evals in `status: failed`** in the eval table while
`legion_evals_processed_total{status="error"}` **never moved**, and the jobs
whose every task had failed to place reported `status: done`.

Two independent gaps produced that:

1. **The job `status` field is a lifecycle flag, not a placement verdict.** A
   job row is inserted with `status = "done"` at submit time, and the only path
   that ever writes `failed` is the KV reactive-update failure path. A job whose
   placements all failed keeps the status it was created with. `GetJobInfo`
   returns the job row and nothing about its allocations, so `legion job info`
   structurally cannot answer "did this place".
2. **Terminal eval failure did not reach the counter.**
   `legion_evals_processed_total{status="error"}` was recorded from the eval
   worker's return value, so the two *retry* dispositions (not-leader and
   plan-conflict, both of which put the eval back on the queue) incremented it
   while the terminal failure path — which writes `status = 'failed'` through a
   different code path — never did. The counter and the table disagreed in both
   directions.

   **Fixed.** `status` is now the eval's disposition — `ok`, `retried`, or
   `failed` — recorded after the disposition is decided, and
   `legion_evals_failed_total{kind,reason}` is incremented *inside* the store
   write that sets the row to `failed`, so no caller can fail an eval without
   counting it. `reason` is `no_capacity`, `unsupported_kind`,
   `conflict_exhausted`, `store_error`, or `other`. If you have a dashboard
   alerting on `status="error"`, move it to `legion_evals_failed_total`;
   `retried` is a contention signal, not a failure one.

   One caveat remains, and `legion scheduler diagnose` handles it for you: the
   counter is per-process while the eval table is replicated, so a failure
   written by another manager in the raft group does not move *this* manager's
   counter. The command reads both and takes the larger.

**What to read instead**

```console
# Did the work actually place? Count allocations, not job status.
$ legion state query "SELECT j.id AS job, j.status, COUNT(a.id) AS allocations \
    FROM job j LEFT JOIN job_config jc ON jc.job_id = j.id \
    LEFT JOIN allocation a ON a.job_config_id = jc.id \
    GROUP BY 1,2 ORDER BY 3 ASC;"

# What failed, and why?
$ legion scheduler diagnose -j | jq '.data.findings[] | select(.code=="invisible_eval_failures")'
$ legion state query "SELECT type AS kind, COUNT(*) FROM eval WHERE status = 'failed' \
    GROUP BY 1 ORDER BY 2 DESC;"
$ legion state query "SELECT id, type AS kind, job_id, node_id FROM eval \
    WHERE status = 'failed' ORDER BY id DESC LIMIT 20;"

# The narrative version, with reasons attached.
$ legion events list --kind eval
$ legion events list --kind allocation
```

`legion state seed-queries` installs `agent:recent-failed-evals` and
`agent:jobs-overview` (job, status, allocation count) as saved queries, which is
the fastest way to get the allocation count next to the status.

**Why an eval fails terminally**

- **Genuinely no capacity.** "no available nodes …" is a terminal failure by
  design. Conflict means transient contention; capacity exhaustion is not a
  conflict, and it does not requeue. Blocked-and-retried capacity waits are the
  [gang-scheduling plan](../internal_docs/plans/gang-scheduling-plan.md)'s
  territory and do not exist yet.
- **Conflict exhaustion does not fail the eval.** It requeues with jitter and
  emits an `eval.requeued` event, counted by
  `legion_eval_conflict_requeues_total`. A rising requeue count with no failures
  is contention, not breakage.
- **An unsupported eval kind** fails immediately, which in practice means a
  mixed-version manager fleet or a corrupt row.

**Known limitation.** The job `status` field is still a lifecycle flag, and
nothing in the product turns "this job's placements all failed" into a job-level
verdict. `invisible_eval_failures` tells you evals failed and in what quantity;
it is cluster-wide, so it cannot name the job. Count allocations, or read
`legion_evals_failed_total{kind,reason}` and follow it into the eval table.
Tracked in [internal_docs/TODO.md](../internal_docs/TODO.md).

---

## Symptom: the cluster is full and no knob helps

**Decision: add capacity.** No scheduler setting applies, and every one of
them makes a genuinely full cluster *slower*.

This section exists because it is the most expensive wrong turn available.
A full cluster and a contended one present almost identically — work is not
placing, rejections may be elevated, latency is up — and the instinct is to
reach for `evalThreads` or `plan_candidates`. On a fleet with no room, more
concurrency means more plans racing for absent capacity, and more candidates
per plan means longer walks that still find nothing. You spend the incident
tuning and the cluster gets worse.

**The reading.**

```console
$ legion scheduler diagnose -j | jq '.data.findings[] | select(.code=="capacity_exhausted")'
```

or by hand:

```console
$ legion node list                    # how many nodes are `available`?
$ legion state query "SELECT type, COUNT(*) FROM eval WHERE status='pending' GROUP BY type;"
```

`capacity_exhausted` fires when **no available node has room and work is
pending**. It also fires when the rejection reason mix
(`legion_plan_rejected_total{reason}`) is dominated by `capacity_shrank`,
`capacity_recheck_budget`, or `late_constraint_failure` — plans that reached
a node and did not fit, rather than plans that lost a race.

**The discriminator.** `node_epoch_moved` dominating the reason mix means
contention, and the remedy is the [rejection
storm](#symptom-throughput-fell-when-i-scaled-up) one. Capacity reasons
dominating means the fleet is the constraint. Same rejection share, opposite
actions — this is exactly why the reason label exists.

**Watch for**: cordoned and draining nodes counting toward "the fleet" in your
head but not toward schedulable capacity. `legion scheduler diagnose` counts
only `available` nodes, because capacity a placement may not use is not
capacity.

---

## The knobs, and when to turn them

| Knob | Where | Turn it when | Do **not** turn it when |
| --- | --- | --- | --- |
| `evalThreads` | manager config | Rejection share is high, or the scheduler lane's `pipeline_wait` is high. **Turn it down.** | You have not looked at `plan_apply_total{outcome}`. Raising it on a contended cluster reliably makes throughput worse. |
| `schedulerSettings.plan_candidates` | manager config | It is `0` and you are not mid-upgrade. Set it back to `8`. | Rejections are low. The default is right. |
| `schedulerSettings.plan_conflict_max_retries` | manager config | Contention is bursty and short-lived (default `3`, max `20`). | Rejections are sustained. More retries wastes more plans. |
| `schedulerSettings.plan_apply_batch_max` | manager config | Group commit is engaging and you want fewer commits (default `16`, max `128`). | `coalesce_batch_size`'s `le="1"` bucket is near `_count`. Batching is not engaging; the cap is not the constraint. |
| `schedulerSettings.score_sample` | manager config | `build_plan` grows with fleet size and you accept approximate score *ranking*. | Any other phase is the one that grew. |
| `heartbeat.targetRatePerSecond` | manager config | Freshness ratio is falling and you want cadence to track fleet size. Off by default. | You are running a published benchmark without recording the served interval and freshness ratio alongside throughput. |
| `heartbeat.missedBeatsBeforeLost` | manager config | Rarely. It multiplies the lost window (default `5`). | You want to relieve manager load. That is the cadence, not the beat count. |
| `liveness.failoverGrace` | manager config | A manager taking over liveness marks nodes lost too eagerly (default `60s`). | Nodes are genuinely gone. |
| `topology = "lanes"` | manager config | The scheduler lane is queued behind status/control work under `single`. | You have one manager and no raft. |
| Raft store on local NVMe | deployment | Always, if it is on network storage. ~86/sec vs ~2,000/sec on identical code. | Never a reason not to. |

---

## What these signals still cannot tell you

Honest gaps, so you do not spend an incident looking for an answer that is not
there. All are tracked in
[internal_docs/TODO.md](../internal_docs/TODO.md).

- **How long work has been queued.** There is no queue-age metric, so a
  764-deep queue that drains in two seconds is indistinguishable from one that
  is six seconds per item. `eval_starvation` therefore fires on depth and kind
  mix rather than on how long anything has actually waited, which makes its
  severity coarser than it should be. The eval table has no creation
  timestamp, so this is a schema change rather than a query. Workaround: watch
  depth over two samples and divide by the processed-eval delta.
- **Whether a job placed.** `legion job info` reports the job row's lifecycle
  status, not placement success — a job whose evals all failed still reports
  `done`. Workaround: count allocations. `invisible_eval_failures` catches the
  failure, but it cannot tell you which job it belonged to;
  cluster-wide-only is a deliberate limit of `legion scheduler diagnose`, and
  the per-job question is `legion job plan`'s.
- **Per-node and per-pool attribution.** Every finding is cluster-wide. A
  report cannot tell you *which* nodes are churning or *which* pool is full
  beyond the free-capacity summary.
- **Anomaly detection.** Every threshold is a constant with a documented
  derivation, not a learned baseline. A cluster whose normal rejection share
  is genuinely 6% will see a standing `warning`.

---

## See also

- [Metrics](metrics.md) — every family, and the manager-wide throughput-ceiling
  read order this page builds on
- [Scheduler and eval loop](scheduler-and-eval-loop.md) — the control loop, the
  epoch fence, group commit, and every `schedulerSettings` knob
- [Late-binding plan apply](late-binding-plan-apply.md) — candidate lists, the
  fallback/reject distinction, and the requeue backstop
- [Heartbeat cadence and scale](heartbeat-cadence-and-scale.md) — sizing, the
  freshness ratio, adaptive cadence
- [Node liveness](node-liveness.md) — where liveness lives and what an election
  does to it
- [Multi-raft](multi-raft.md) — lanes, the one-thread-per-lane pipeline, and
  lane isolation
- [Scheduling indexes](scheduling-indexes.md) — bounding candidate construction
- [Testing and benchmarks](testing-and-benchmarks.md) — reproducing any of this
  on a rig
