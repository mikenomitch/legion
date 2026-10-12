# Diagnosing Scheduler Performance

Use this page when placements are slow, throughput dropped, or work isn't
running and nothing obvious is broken. It's organized by symptom. Find your
row, read the decision, and take the action.

The worked examples below are measurements from Legion scale tests.

> **Never act on a single number.** Every metric on this page has a
> **neighbour** metric that tells you what it means. Reading `plan_apply`
> latency without `pipeline_wait`, or a slow placement rate without the
> freshness ratio, will point you at the wrong cause. In our scale tests, every
> wrong turn came from reading one number without its neighbour.

---

## Start here

| What you are seeing | Go to |
| --- | --- |
| Throughput **fell** when I added eval threads, or when the cluster grew | [Rejection storm](#symptom-throughput-fell-when-i-scaled-up) |
| Throughput is capped but the box is **idle** | [Latency-bound](#symptom-the-box-is-idle-and-throughput-is-still-capped) |
| Everything got slower **in proportion to fleet size** | [Phase growth](#symptom-it-got-slower-as-the-cluster-grew) |
| New submissions take seconds while the cluster is otherwise busy | [Eval starvation](#symptom-new-work-waits-behind-a-backlog-of-other-work) |
| Placements look glacial and nodes keep going `lost` | [Heartbeat ceiling](#symptom-slow-placements-and-nodes-flapping-lost) |
| Nothing is running, and nothing reports an error | [Invisible failures](#symptom-nothing-is-running-and-nothing-reports-an-error) |

Before you go to any of them, spend thirty seconds checking whether the
scheduler is the right place to look:

```console
$ legion node cadence          # is the control plane behind on liveness?
$ curl -s localhost:4749/metrics | grep -E 'legion_(runtime_spawn_delay|grpc_in_flight|runtime_worker_threads)'
```

- A **freshness ratio below 0.5** means a liveness problem is creating
  scheduler load. Fix that first
  ([Heartbeat ceiling](#symptom-slow-placements-and-nodes-flapping-lost)).
- **`legion_runtime_spawn_delay_seconds` in the milliseconds** means blocking
  handlers have saturated the tokio runtime, and every RPC is slow, scheduler
  or not. That's a manager-wide problem. Follow the steps in
  [Metrics](metrics.md#diagnosing-a-manager-throughput-ceiling).

If both look fine, the problem is in the scheduler and the sections below apply.

### Reading counters correctly

Every `_total` family is cumulative since process start. If a manager has been
up for six hours, a ratio over its whole lifetime won't show a storm that
started ten minutes ago. Take two samples and subtract:

```console
$ curl -s localhost:4749/metrics | grep plan_apply_total > /tmp/a
$ sleep 30
$ curl -s localhost:4749/metrics | grep plan_apply_total > /tmp/b
$ diff /tmp/a /tmp/b
```

The same applies to every histogram. `sum / count` since boot is an average
over the whole run, which can differ a lot from the current latency. Subtract
two samples of both.

---

## Symptom: throughput fell when I scaled up

**Decision: check the rejection share first.** If it is high, reduce
`evalThreads`, or let the manager reduce it for you, which is what
[adaptive scheduler concurrency](adaptive-scheduler-concurrency.md) does, and
only while the rejection share is actually high. If it isn't, `evalThreads` is
the wrong knob to lower, and raising it is more likely to help.

> **More eval threads usually help.** An earlier version of this page said to
> reduce concurrency, based on a test where 4 eval threads beat 16. That test
> had two flaws. Its load generator couldn't submit work as fast as the
> scheduler could absorb it. And the build it ran woke every eval worker on any
> eval state change, so each added thread spent quorum rounds finding an empty
> queue. With both fixed, throughput rises with eval threads on the same
> hardware (5,000 nodes, 25,002 allocations, three 4-vCPU managers):
>
> | eval threads | throughput |
> | ---: | ---: |
> | 4 | 177.27 alloc/s |
> | 8 | 208.83 alloc/s |
> | 16 | 219.25 alloc/s |
> | 32 | 251.68 alloc/s |
>
> p99 placement latency fell from 100.8s to 67.3s between 4 and 16. A
> 64-thread run reached at least 263.08 alloc/s but was limited by the
> benchmark's own load generator, so we don't know where adding threads stops
> helping. Read the table as "more threads helped up to 32 on a 4-core
> manager." It is not a recommendation to set 32.

**Read**

```
legion_plan_apply_total{outcome="rejected"}
legion_plan_apply_total{outcome="applied"}
legion_plan_apply_total{outcome="fallback"}
```

Compute the rejection share over a window (not since boot):
`rejected / (applied + fallback + rejected)`.

| Rejection share | Meaning |
| --- | --- |
| under 5% | Normal contention. Ignore it. |
| 5–20% | Every rejection is a wasted plan plus a full eval retry. Worth acting on. |
| over 20% | Collapse. You're spending more work on losing races than on placing. |

**Neighbour metrics to check**

- `legion_plan_apply_total{outcome="fallback"}` high and `rejected` low means
  late-binding candidate lists are absorbing the races. This is working as
  designed, so do nothing. See [Late-binding plan apply](late-binding-plan-apply.md).
- `legion_scheduler_plan_candidate_rank` showing deep ranks alongside high
  `rejected` means the pool is effectively full. Add capacity. No scheduler
  knob will fix a full pool.
- `legion_scheduler_index_candidates{index}` showing plenty of candidates
  while rejections climb means the plan has enough candidates. It's losing
  them to a concurrent apply, which is the case this section covers.
- CPU rising while placements fall is the telltale sign. Contention burns CPU
  on work that gets thrown away.

**Worked example**

Three test runs on an older build:

| Eval threads | Nodes | `rejected` share | Placements | Manager CPU |
| ---: | ---: | ---: | ---: | ---: |
| 4 | 1,000 | 15.9% | 354.6 alloc/sec | 29% |
| 4 | 5,000 | 31.9% | n/a | n/a |
| 16 | 5,000 | 73.6% | 310.4 alloc/sec | 85% |

At 73.6%, three of every four plans were discarded and their evals retried.
The rejection messages read "no candidate for task N could bind: all 1
candidate(s) were stale or invalid." Meanwhile, the scheduling index was
giving placement 33 to 64 candidates per task. That shows the plans were
losing races, not running out of nodes.

These runs predate the two-pass bind and the fix that stopped every eval
worker waking on each eval state change. They also used a load generator that
capped submissions. So don't read the placements column as a comparison of
thread counts. The corrected measurement above is the one to use. The two-pass
bind ([late-binding-plan-apply.md](late-binding-plan-apply.md)) removed the
cause of these rejections. Now, a candidate whose node epoch moved is revalidated against
committed state and bound if it still fits, so contention no longer turns into
rejections. If your build has the two-pass bind, check
`legion_scheduler_plan_bind_pass{pass="revalidated"}` before changing
`evalThreads`. A large `revalidated` count with a flat `rejected` counter
means the contention is being absorbed, and the remaining rejections are
actual capacity limits.

**Actions, in order**

1. **Let the manager pick the number for you.** Turn on adaptive scheduler
   concurrency and leave `evalThreads` at your core count: the manager measures
   its own conflict rate and moves the number of concurrent planners between a
   floor and that ceiling, so nobody has to bisect.

   ```nickel
   # configs/manager.ncl
   { schedulerSettings = { adaptiveConcurrency = { mode = "enforce" } } }
   ```

   It defaults to `observe`, which computes and exports the limit it *would*
   enforce without acting. Run that first on your own workload and read
   `legion scheduler concurrency`. It also answers the question the manual
   bisect cannot: whether your conflicts are concurrency-driven at all, or
   whether the pool is simply full (state `saturated`, which means add nodes).
   See [adaptive-scheduler-concurrency.md](adaptive-scheduler-concurrency.md).

2. **Or lower `evalThreads`** by hand and re-measure, but only if the
   rejection share above is high. Start at half and bisect.

   ```nickel
   # configs/manager.ncl
   { evalThreads = 4 }
   ```

   The default is `available_parallelism()`. Core count is a poor guide.
   An eval worker spends most of its time waiting on a quorum round trip
   instead of computing (a saturated 5,000-node control plane measured 8%
   CPU). The useful thread count depends on how much the plan-apply fence can
   absorb and on round-trip latency, not on cores. On an uncontended workload,
   the default is more likely too low than too high. At 5,000 nodes on 4-vCPU
   managers, throughput rose at every step from 4 to 32 threads. We haven't
   measured where it stops, so larger values are worth trying and measuring,
   but they are not a recommendation.

   `evalThreads` also sizes the store's read-connection pool (`evalThreads + 4`,
   capped at 16, see [state-store.md](state-store.md)), because the eval
   workers are the manager's concurrent readers. Turning it down also narrows
   the pool. Watch `legion_store_read_lock_wait_seconds` alongside whatever you
   were trying to fix.

3. **Confirm candidate lists are on.** `schedulerSettings.plan_candidates`
   defaults to `8`. Setting it to `0` disables them and goes back to
   single-node submissions, which turns every race into a rejection. If someone
   set it to `0` for a mixed-version upgrade window and never set it back,
   that alone can cause this symptom.

4. **Don't raise `plan_conflict_max_retries` first.** More retries means more
   plans built against a snapshot that is already losing. It moves the wasted
   work around without reducing it.

**Known limitation.** Legion can't yet tell you why a plan was rejected. A
stale job-config epoch, a node whose epoch moved under a concurrent apply, a
cordon, a shrunken node, and a static-port collision all land in the same
`rejected` counter. Only the free-text rejection string on the
`plan_submission` row tells them apart. Until there is a counter labelled by
reason, read the strings.

The strings include node and task ids, so grouping on the raw column gives one
group per row. Classify them instead:

```console
$ legion state query "SELECT CASE \
    WHEN rejection_reason LIKE '%plan epoch for job config%' THEN 'job_epoch_stale' \
    WHEN rejection_reason LIKE '%epoch moved%'               THEN 'node_epoch_moved' \
    WHEN rejection_reason LIKE '%cordoned, draining, or lost%' THEN 'node_unavailable' \
    WHEN rejection_reason LIKE '%no longer has capacity%'    THEN 'capacity_shrank' \
    WHEN rejection_reason LIKE '%does not satisfy constraints%' THEN 'late_constraint' \
    WHEN rejection_reason LIKE '%no longer exists%'          THEN 'node_gone' \
    WHEN rejection_reason LIKE '%capacity-recheck budget%'   THEN 'recheck_budget' \
    ELSE 'other' END AS reason, COUNT(*) AS n \
    FROM plan_submission WHERE status = 'rejected' GROUP BY 1 ORDER BY 2 DESC;"
```

Each reason has a different remedy, so classify before you act:

| Reason | Cause | Remedy |
| --- | --- | --- |
| `node_epoch_moved` | A concurrent apply took the node and the placement couldn't be revalidated onto it. Only an edge-boot ratification (`target_instance`) rejects on epoch alone. | Rare. If you see many of these, look at `speculativeStart` jobs instead of `evalThreads`. |
| `job_epoch_stale` | Two plans raced on one job config | Reduce concurrency. Job-config epochs are a strict all-or-nothing fence. |
| `node_unavailable` | Cordoned, draining, or lost since the plan snapshot | This is fleet churn. Reducing `evalThreads` won't help. Find what is cordoning or losing nodes. |
| `capacity_shrank` | A worker re-registered with smaller totals | Check worker configs and re-registrations. |
| `late_constraint` | Constraints or permits no longer satisfied | Check node attribute churn. |
| `node_gone` | The node row vanished mid-plan | Usually deregistration or a wiped fleet. |
| `recheck_budget` | The plan ran out of its per-plan capacity-recheck budget | Contention plus long candidate lists. Reduce concurrency. |

---

## Symptom: the box is idle and throughput is still capped

**Decision: don't resize the instance.** A bigger box won't help. The work is
waiting in a queue, and the CPU is mostly idle.

**Read**

```
legion_raft_pipeline_wait_seconds{lane="scheduler"}
legion_raft_pipeline_wait_seconds{lane="status"}
legion_raft_propose_apply_seconds{lane}
legion_raft_pipeline_depth{lane}
```

Each raft lane is served by one OS thread (capture, propose, and apply run in
sequence per lane, see [Multi-raft](multi-raft.md)). Time spent queued in
front of that thread is a major part of latency. Other histograms don't show
it, because they only start timing once the thread picks up a job.

**Neighbour metrics to check**

| If | Then |
| --- | --- |
| `pipeline_wait` grew, `propose_apply` flat | The lane's single pipeline thread is the constraint. Replication is fine. More CPU won't help. |
| `propose_apply` grew, `pipeline_wait` flat | Replication is the constraint: network, fsync, or a lagging follower. Faster disk or better-connected voters help. |
| `legion_raft_capture_seconds` grew | Local SQLite work grew. Look at read-model size and the apply phase. |
| None of the three moved | The time is outside the store. Check `legion_runtime_spawn_delay_seconds` next. |

**Worked example**

A control plane at 29% CPU was capped on placement throughput. No thread was
above 40% of a core, and `legion_runtime_spawn_delay_seconds` stayed under
50µs. The runtime wasn't saturated and no handler was slow. One comparison
showed the cause:

| Lane | `pipeline_wait` per op | `propose_apply` |
| --- | ---: | ---: |
| scheduler | **3.07 ms** | ~1.5 ms |
| status | 0.09 ms | ~1.5 ms |

The scheduler lane queued 34x longer than the status lane, with the same
replication cost and similar volume. In total, the scheduler lane spent 206
seconds in pipeline wait over the run. The cause was op count: 67,165 raft ops
for roughly 10,700 evals, or about six replicated proposals per eval
(`claim_eval`, the plan apply, `complete_eval`, and enqueues, each its own
proposal). Meanwhile, `legion_scheduler_plan_phase_seconds{phase="total"}`
averaged 9.32ms per eval, while actual wall time was roughly 75ms per eval per
thread. The scheduling work wasn't the problem. The queue in front of the lane
was.

**Actions**

1. **Cut proposals per eval before cutting threads.** More eval threads do put
   more proposals into the same single-threaded lane. But proposals per eval
   are cheaper to reduce, and much of the six counted here has since been
   removed. `claim_eval` and `complete_eval` are now a single fused round
   (`complete_and_claim_next`), enqueues are batched, and extra threads no
   longer each issue their own empty `claim_eval` on wake-up. Read
   `legion_raft_ops_total{lane,op,outcome}` and check whether the ops are
   `applied` or `empty`. An `empty` changeset never reaches quorum, so a high
   op rate there costs nothing in replication. Reduce `evalThreads` only if
   the lane's `applied` rate is the constraint.
2. **Check topology.** Under `topology = "single"`, every lane shares one raft
   group, so scheduler work queues behind status and control work. `lanes`
   gives the scheduler its own pipeline thread. See [Multi-raft](multi-raft.md).
3. **Check `legion_store_coalesce_batch_size{path="plan_apply"}`.** If the
   `le="1"` bucket is close to `_count`, group commit isn't engaging and each
   plan pays for a full commit. Batching only forms when submitters overlap,
   so having it configured doesn't mean it's happening. This histogram is the
   only way to tell.
4. **Don't** buy a bigger instance when you see this pattern. Nothing here is
   CPU-bound.

**Known limitation.** Each lane has one pipeline thread. This is a limit of
the current architecture.

---

## Symptom: it got slower as the cluster grew

**Decision: find the phase that grew, then act on that phase.** Phases don't
grow evenly, and each one has a different remedy.

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
| `available_nodes` | 0.03 ms | n/a | flat |

A 5x fleet produced a 33x `task_fetch`. That superlinear growth is what
matters here, even though `apply` is the largest absolute number. Looking only
at the biggest number would have pointed the investigation at the apply path,
which grew the least in relative terms.

**What each phase growing means**

| Phase | Growing means | Do |
| --- | --- | --- |
| `available_nodes` | The node snapshot is being rebuilt instead of shared by reference. Some change is invalidating the cached list on every plan. | Look for constant node churn: registrations, cordons, availability flapping. |
| `version_sample` | Reading the fence epochs from the plan's own snapshot is taking longer than it should. It is a small, fixed cost per plan. | Capture the numbers and file a bug. See [rejection storm](#symptom-throughput-fell-when-i-scaled-up) if rejections are also climbing. |
| `task_fetch` | Task-config loading is scaling with something it shouldn't. Superlinear growth here points to a bug, not a capacity limit. | Capture the numbers and file a bug. No config knob fixes this. |
| `capacity_snapshot` | Allocation count per plan is growing. Expected as the cluster fills. | Nothing to tune. This is necessary work. |
| `build_plan` | Candidate construction and scoring are growing with fleet size. This is expected, and it's the one phase with a direct knob. | Set `schedulerSettings.score_sample` to bound scoring cost, declare a [scheduling index](scheduling-indexes.md), or both. |
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

Feasibility is never sampled. Constraints, permits, distinct rules, and
resource fit always run in full, so a sampled placement is always valid. Only
the score ranking is approximate. See
[Scheduler and eval loop](scheduler-and-eval-loop.md#bounded-candidate-scoring-score_sample).

---

## Symptom: new work waits behind a backlog of other work

**Decision: this is a queue ordering problem.** Don't add eval threads.
Confirm the mix of eval kinds first.

**Read**

```
legion_evals_processed_total{kind}      # by kind, over a window
legion_eval_queue_depth
```

**Worked example**

After a mass node-loss event:

| Eval kind | Processed |
| --- | ---: |
| `node_lost_reschedule` | 5,760 |
| `job_submit` | 2,503 |

`legion_eval_queue_depth` was 764, and job submissions took 6 seconds each.
The scheduler wasn't slow. It was busy with recovery work, and submissions
couldn't get ahead of it. `claim_eval` orders by `priority DESC, id ASC`, and
a `node_lost_reschedule` enqueued with no priority gets the same default (50)
as a normal job. At equal priority the autoincrement id decides, so a new
submit can never overtake an older backlog.

**Known limitation, and the workaround**

`legion_eval_queue_depth` is a single unlabelled gauge. It tells you the queue
is 764 deep but not what is in it, which is what you need here. Until it has a
`kind` label, read the queue contents from the store directly:

```console
$ legion state query "SELECT kind, status, COUNT(*) FROM eval \
    WHERE status IN ('pending','blocked') GROUP BY 1,2 ORDER BY 3 DESC;"
```

`legion state seed-queries` installs `agent:pending-evals` and
`agent:recent-failed-evals`. These are saved queries that give the same
information to agents and the UI State Explorer.

**Actions**

- **Raise the priority of the work you care about**, per submission, with
  `legion job run --priority N` or `--asap`. This is the only option that works
  today. A job's [priority band](preemption-and-eviction.md) also sets the
  default queue priority for its evals.
- **Expect this after any large node event.** Losing one AZ in a three-AZ
  cluster takes out about 33% of the fleet. That's under the 50%
  mass-disruption circuit breaker, so all the reschedule evals fire. See
  [Heartbeat cadence and scale](heartbeat-cadence-and-scale.md#safety-valves).
- **Don't expect eval threads to fix this.** More threads never let a new
  submission overtake an older eval of equal priority. They can drain the
  backlog faster, but only while the rejection share stays low and the
  scheduler lane isn't saturated with `applied` ops (see
  [above](#symptom-the-box-is-idle-and-throughput-is-still-capped)). Check
  both before raising `evalThreads`.

`claim_eval` doesn't yet balance fairly across eval kinds, and lost nodes with
no allocations still cost a full reschedule eval.

---

## Symptom: slow placements and nodes flapping `lost`

**Decision: check the freshness ratio before you touch anything in the
scheduler.** A control plane that is behind on heartbeats looks like a slow
scheduler, but the fix is in a different subsystem.

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

**`legion_heartbeat_freshness_ratio` tells the two cases apart.** False node
loss begins when it falls below `served_interval / lost_window`, which is 0.20
at the defaults. Below that line, healthy nodes get marked lost. That creates
reschedule evals, which add load and lower the ratio further. In the
scheduler metrics, this feedback loop looks like the scheduler getting slower.

**Worked example**

A test run reported 0.9 placements/sec at a p50 of 109 seconds, and those
numbers were first read as scheduler throughput. They weren't. The fleet had a
0.22 freshness ratio against a 0.20 false-loss threshold, about 10% away from
marking a healthy fleet dead. The measured heartbeat rate was ~80/sec, against
the ~500/sec that a 1,000-node fleet at a 2s cadence needs. Nothing was
saturated: manager CPU was 19–23%, sim hosts were at 0.2%, and
`RaftTransport/AppendEntries` answered in 3ms. Those placement figures
measured the heartbeat ceiling, so they can't be used as scheduler numbers.

**Neighbour metrics to check**

- `legion_nodes_marked_lost_total` rising while the machines are clearly alive
  is false loss. The problem is in the control plane, not the fleet.
- `legion_liveness_loss_suppressed_total{reason}` non-zero means a safety valve
  fired: `mass_disruption` (over half the fleet would have been lost in one
  tick), `failover_grace` (a manager just took over liveness), or
  `peer_digest_gap`. This counter tells "nothing was marked lost" apart from
  "nothing was wrong."
- `legion_liveness_digest_rpcs_total{outcome="unimplemented"}` non-zero means
  an older manager without liveness digests leads the status lane, and
  heartbeats have fallen back to replicated writes. Finish the upgrade.
- `legion_nodes{status="lost"}` counts nodes that are currently away.
  `legion_nodes_marked_lost_total` counts each time a node went away. You need
  the second one to see flapping.

**Actions**

1. **Use the value `legion node cadence` suggests.** It computes
   `heartbeatSeconds` for this fleet size on this control plane at the
   configured headroom. Set it on the worker type.
2. **Or lower the adaptive target** so cadence backs off sooner. Adaptive
   cadence is on by default at 2,500 beats/s, which keeps fleets up to 2,500
   nodes at 2s. A control plane that falls behind below that needs a lower
   target:

   ```nickel
   # configs/manager.ncl
   heartbeat = {
     targetRatePerSecond = 80,   # measure yours; default 2500
   }
   ```

   The lost window always moves with the cadence
   (`missedBeatsBeforeLost x interval`), so backing off keeps the same
   detection margin. If `legion node cadence` says `workers use their
   worker-type heartbeatSeconds`, someone set `heartbeat.adaptive = false`,
   and nothing adapts.
3. **Accept the cost.** Detection slows down in proportion to the back-off. At
   1,000 nodes and R=80, Legion takes up to 125 seconds instead of 10 to notice
   a dead machine.

Full model, sizing formula, and measurement recipe:
[Heartbeat cadence and scale](heartbeat-cadence-and-scale.md).

**Check your storage.** With the same code and topology, one control plane
sustained ~86 heartbeat commits/sec on EBS gp3 and ~2,000 on local NVMe. If
your raft store is on network storage, move it first. No scheduler metric will
show you this.

---

## Symptom: nothing is running and nothing reports an error

**Decision: read `legion job info`'s status, and don't trust
`evals_processed_total{status="error"}`.** A job with nothing placed reads
`pending`, but the error counter doesn't report placement failure. Count
allocations to see how much placed.

**Worked example**

A run ended with 264 evals in `status: failed` in the eval table, while
`legion_evals_processed_total{status="error"}` never moved. Jobs where every
task had failed to place read `pending`.

Two things explain that:

1. **`pending` means nothing of the job is live.** The job status is derived
   from the workload (see
   [Job Status](jobs-tasks-and-scheduling.md#job-status)): with no live
   allocation and a task that wants one, the job reads `pending`. That covers
   both "not placed yet" and "can't be placed", so read the eval table for
   the reason (below). The state store's
   `job.status` column is a lifecycle flag (`active`) and says nothing about
   placement, so don't read it for this.
2. **Terminal eval failures don't reliably reach the counter.**
   `legion_evals_processed_total{status="error"}` is recorded from the eval
   worker's return value. The two retry outcomes (not-leader and
   plan-conflict, which both put the eval back on the queue) also increment
   it. Meanwhile, the terminal failure path writes `status = 'failed'` to the
   eval row through a different code path. So the counter and the table
   disagree in both directions.

**What to read instead**

```console
# Which jobs have nothing live? (derived status)
$ legion job list --status pending

# How much placed? Count allocations. (j.status here is the lifecycle flag.)
$ legion state query "SELECT j.id AS job, j.status, COUNT(a.id) AS allocations \
    FROM job j LEFT JOIN job_config jc ON jc.job_id = j.id \
    LEFT JOIN allocation a ON a.job_config_id = jc.id \
    GROUP BY 1,2 ORDER BY 3 ASC;"

# What failed, and why?
$ legion state query "SELECT kind, COUNT(*) FROM eval WHERE status = 'failed' \
    GROUP BY 1 ORDER BY 2 DESC;"
$ legion state query "SELECT id, kind, job_id, node_name FROM eval \
    WHERE status = 'failed' ORDER BY id DESC LIMIT 20;"

# The narrative version, with reasons attached.
$ legion events list --kind eval
$ legion events list --kind allocation
```

`legion state seed-queries` installs `agent:recent-failed-evals` and
`agent:jobs-overview` (job, lifecycle flag, allocation count, live count) as saved queries. This is
the fastest way to see the allocation count next to the status.

**Why an eval fails terminally**

- **No capacity.** "no available nodes …" is a terminal failure by design. A
  conflict means short-lived contention, but running out of capacity is not a
  conflict, so the eval doesn't requeue. Legion doesn't yet support waiting
  and retrying when capacity is short.
- **Running out of conflict retries doesn't fail the eval.** It requeues with
  jitter and emits an `eval.requeued` event, counted by
  `legion_eval_conflict_requeues_total`. A rising requeue count with no
  failures means contention. Nothing is broken.
- **An unsupported eval kind** fails immediately. In practice this means a
  mixed-version manager fleet or a corrupt row.

**Known limitation.** Until failed evals reach `evals_processed_total`, a
dashboard built only on that counter will show a clean cluster while work is
failing. Alert on the eval table, or on the gap between desired and live
allocations, instead of on the counter.

---

## The knobs, and when to turn them

| Knob | Where | Turn it when | Do **not** turn it when |
| --- | --- | --- | --- |
| `evalThreads` | manager config | Rejection share is high, or the scheduler lane's `pipeline_wait` is high, and the lane's ops are `applied` instead of `empty`. In those cases, turn it down, or set `adaptiveConcurrency.mode = "enforce"` and let the manager find the level below it. On an uncontended cluster, the opposite applies. At 5,000 nodes, throughput rose at every step from 4 to 32 threads (177.27 -> 251.68 alloc/s), and we haven't measured where that stops. It also sizes the read pool (`evalThreads + 4`, capped at 16). | You haven't looked at `plan_apply_total{outcome}` and `raft_ops_total{op,outcome}`. Without those, changing it in either direction is guessing. |
| `schedulerSettings.adaptiveConcurrency.mode` | manager config | You're tuning `evalThreads` by hand, or you can't tell contention from a full pool. `observe` first, then `enforce`. | It's never a bad time to run `observe`. It changes nothing ([adaptive-scheduler-concurrency.md](adaptive-scheduler-concurrency.md)). |
| `schedulerSettings.plan_candidates` | manager config | It's `0` and you're not mid-upgrade. Set it back to `8`. | Rejections are low. The default is right. |
| `schedulerSettings.plan_conflict_max_retries` | manager config | Contention is bursty and short-lived (default `3`, max `20`). | Rejections are sustained. More retries wastes more plans. |
| `schedulerSettings.plan_apply_batch_max` | manager config | Group commit is engaging and you want fewer commits (default `16`, max `128`). | `coalesce_batch_size`'s `le="1"` bucket is near `_count`. Batching isn't engaging, so the cap isn't the constraint. |
| `schedulerSettings.job_intake_coalescing` | manager config | Only to turn off job-intake group commit (default `true`) while diagnosing a submit-path incident. It's an on/off switch with nothing to tune. | You're chasing submit throughput. Turning it off always means more quorum rounds per job. |
| `schedulerSettings.score_sample` | manager config | `build_plan` grows with fleet size and you accept approximate score ranking. | Any other phase is the one that grew. |
| `heartbeat.targetRatePerSecond` | manager config | Freshness ratio is falling below 2,500 nodes, so cadence must back off sooner (lower it), or you need faster detection on a large fleet and have measured the headroom (raise it). Default `2500`. | You're running a published benchmark without recording the served interval and freshness ratio alongside throughput. |
| `heartbeat.adaptive` | manager config | Only to pin every worker to its worker-type `heartbeatSeconds` (default `true`), for example in a run whose subject is a fixed cadence. | You want less heartbeat load. Adaptive cadence is what reduces it. |
| `heartbeat.missedBeatsBeforeLost` | manager config | Rarely. It multiplies the lost window (default `5`). | You want to reduce manager load. Change the cadence for that instead. |
| `liveness.failoverGrace` | manager config | A manager taking over liveness marks nodes lost too eagerly (default `60s`). | Nodes are actually gone. |
| `topology = "lanes"` | manager config | The scheduler lane is queued behind status/control work under `single`. | You have one manager and no raft. |
| Raft store on local NVMe | deployment | Always, if it's on network storage. ~86/sec vs ~2,000/sec on the same code. | No exceptions. |

---

## What these signals still cannot tell you

These are the current gaps, so you don't spend an incident looking for an
answer that isn't there.

- **Why a plan was rejected.** `legion_plan_apply_total{outcome="rejected"}`
  has no reason label. A stale job-config epoch, a node epoch moved by a
  concurrent apply, a cordon, a shrunken node, a late constraint failure, and
  a static-port collision are six different problems with six different
  remedies, and they share one counter. Workaround: group
  `plan_submission.rejection_reason`.
- **What is in the eval queue.** `legion_eval_queue_depth` is one unlabelled
  gauge. Workaround: `GROUP BY kind` on the eval table.
- **How long work has been queued.** There is no queue-age metric, so you
  can't tell a 764-deep queue that drains in two seconds from one that takes
  six seconds per item. Workaround: watch depth over two samples and divide by
  the processed-eval delta.
- **Whether a job placed.** `legion job info` reports the job row's lifecycle
  status, not placement success. Workaround: count allocations.
- **Whether evals are failing.** Terminal failures don't reliably reach
  `evals_processed_total{status="error"}`. Workaround: query the eval table.
- **Whether nodes came back.** `legion_nodes_marked_lost_total` has no
  `legion_nodes_recovered_total` counterpart. A fleet that dies and recovers
  looks like nothing happened, and you can't see flapping. Workaround: watch
  `legion_nodes{status="lost"}` transitions over time.

---

## See also

- [Metrics](metrics.md): every family, and the manager-wide throughput-ceiling
  steps this page builds on
- [Scheduler and eval loop](scheduler-and-eval-loop.md): the control loop, the
  epoch fence, group commit, and every `schedulerSettings` knob
- [Late-binding plan apply](late-binding-plan-apply.md): candidate lists, the
  fallback/reject distinction, and the requeue backstop
- [Heartbeat cadence and scale](heartbeat-cadence-and-scale.md): sizing, the
  freshness ratio, adaptive cadence
- [Node liveness](node-liveness.md): where liveness lives and what an election
  does to it
- [Multi-raft](multi-raft.md): lanes, the one-thread-per-lane pipeline, and
  lane isolation
- [Scheduling indexes](scheduling-indexes.md): bounding candidate construction
- [Testing and benchmarks](testing-and-benchmarks.md): reproducing any of this
  on a test rig
