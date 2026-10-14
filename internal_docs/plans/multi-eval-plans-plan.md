# Plan: More Placements per Plan Entry

> **Judgement calls:** [`calls/multi-eval-plans-plan_calls.md`](../calls/multi-eval-plans-plan_calls.md)

**Status: IN EXECUTION** (written 2026-10-13). Phases 1 and 2 shipped
together: one running eval per job, and the fused settle in every placing
handler whose plan is its last write. Phase 3 (settle the evals a running
plan already covered, without a handler of their own) is planned and
deferred until a measurement says the no-op evals it removes are worth
their entries. Results are in [§12](#12-results).

## 1. Problem

Every keyed sandbox step that places work queues an eval. `CreateInstance`
and a resume each enqueue a `job_reschedule` for the instance's job
(`src/manager/server.rs`, `create_instance`, and
`src/manager/server/suspend_rpcs.rs`), and that eval reconciles the whole
job: a reschedule plan adds every missing keyed allocation, not one.

Under load the eval loop did not use that. Each create's eval reached an
idle worker within a millisecond, so a burst of creates for one job ran as
many concurrent handlers for the same job. They all planned the same
pending instances, and the job-config epoch fence
(`apply_plan_submission_on`, "a hard, all-or-nothing check: it is what
serializes two plans for the SAME job config") let exactly one commit. The
rest were rejected. A rejection still commits (the `rejected` submission
row and its event), so it is a scheduler-lane entry of its own. Then the
loser backed off 25 to 60 ms and planned again, usually against the next
winner.

The sandbox-scaling plan measured the symptom without naming the cause:
plan applies averaged 1.09 plans per entry, one eval each, and the
scheduler lane (about 6 ms of serial time per entry) bounded keyed
lifecycles at about 50 a second
([sandbox-scaling-plan.md §14.3](sandbox-scaling-plan.md#143-phase-1-group-commit-and-write-trimming),
[calls #17](../calls/sandbox-scaling-plan_calls.md)). The lane cost is per
entry, not per placement, so the cheapest entry is the one a burst never
writes.

A second, smaller cost: only `job_reschedule` settled its eval inside its
plan's entry. `job_submit`, `job_update`, and `job_scale` paid a separate
settle entry after every plan
([calls #23](../calls/sandbox-scaling-plan_calls.md)).

## 2. Prior art

- **Nomad's eval broker** keeps at most one outstanding evaluation per job.
  Evals that arrive for a job with one in flight wait in a per-job pending
  set. When the outstanding one is acked, the broker releases the next and
  marks the older duplicates cancelable, and the leader reaps them
  ([`nomad/eval_broker.go`](https://github.com/hashicorp/nomad/blob/main/nomad/eval_broker.go),
  `blocked` by job and `cancelable`). *Take:* serialize a job's evals at
  the queue and collapse what waited. *Reject:* a second "cancelable" state
  and a reaper. Legion's claim already subsumes duplicates and its settle
  deletes them in the same write.
- **Kubernetes `client-go` workqueue** guarantees that one key is never
  processed by two workers at once, and that a key added several times
  while it waits or runs is processed once more, not once per add
  ([`util/workqueue/queue.go`](https://github.com/kubernetes/client-go/blob/master/util/workqueue/queue.go),
  the `dirty` and `processing` sets). Every controller relies on it to turn
  a storm of events into a few reconciles. *Take:* exactly this contract,
  with the job as the key. A Legion reschedule is a reconcile of its job,
  the same shape as a controller's sync.
- **Kubernetes scheduler** places one pod per scheduling cycle and binds
  asynchronously. Batching is left to the controllers above it.
  *Reject:* per-instance plans. Legion's planner already places a whole
  job in one plan, so the batch unit exists. What was missing was letting
  it fill.

## 3. Jobs to be done

**Platform builder (job author running a sandbox product)**
- When hundreds of my users open a sandbox in the same second, I want them
  all to start quickly, so a burst does not turn into a queue.
- When traffic is light, I want each sandbox placed the moment it is asked
  for, so batching never costs me latency I can see.

**Cluster operator**
- When one tenant bursts, I want everyone else's work to keep flowing, so a
  noisy job cannot take every scheduler worker.
- When placement slows down, I want to see whether work is being folded
  together or retried, so I can tell a busy cluster from a sick one.

**On-call responder**
- When a job looks stuck, I want its waiting work to show as waiting, not
  failed, so I do not restart something that is fine.

*Not in scope:* the autonomous agent persona. No RPC, CLI, or MCP surface
changes. Agents see the same instance and eval APIs, only faster.

### User scenarios

1. A coding-agent platform opens 300 sandboxes for one task in a second.
   Each `CreateInstance` returns as before. Placements land in a handful of
   plans, not 300, and none is rejected.
2. The same cluster runs 40 other jobs. Their evals keep being claimed
   while the burst drains, because the burst holds one worker, not all of
   them.
3. A quiet cluster gets one create a minute. It is placed in one entry,
   exactly as before.

## 4. Goals and non-goals

**Goals**
- Placements per plan entry grow with the arrival rate for one job.
- No plan for a job is ever rejected by another plan for the same job on
  the same manager.
- An idle cluster keeps single-eval latency: nothing waits to fill a batch.
- No new API. One escape-hatch setting.

**Non-goals**
- Batching plans of *different* jobs into one entry. The plan-apply group
  commit already does that, and its cap and ratification share belong to
  another change.
- Changing the scheduler lane's per-entry mechanics (capture, pipelining)
  or plan-commit cost that grows with fleet size.
- Cross-manager exclusivity. Only the scheduler lane's leader runs evals.
- A wait window to fill batches. Zero wait is a goal, not a knob.

## 5. What exists today

- **Claims are leader memory** (`src/storage/store/eval_claims.rs`). A claim
  takes the head of `priority DESC, id ASC` and subsumes pending duplicates
  (same kind, same job, config, and node, already ready). The settle deletes
  them in its write.
- **The fused settle** (`arm_eval_settle`, `src/storage/store.rs`): a
  handler that declares its plan its last write has the plan delete the
  eval row and the subsumed duplicates, and stage `eval.completed`, in the
  plan's entry. Only `job_reschedule` armed it
  (`src/structs/eval.rs`, `handle_job_reschedule`).
- **The eval worker loop** (`src/manager/daemon.rs`, `eval_worker_loop`)
  claims a fair-share batch, runs each handler, then settles the batch and
  claims the next in one call (`complete_evals_and_claim_next`).
- **The epoch fence** (`apply_plan_submission_on`) rejects a plan whose
  job-config epoch moved since its snapshot. Rejections commit.
- **Conflict retries** (`plan_with_conflict_retries`, `src/structs/eval.rs`):
  3 attempts, 25 ms base backoff plus jitter, then a 1 to 2 s requeue.
- **The redundant settle.** A settled eval whose claim subsumed duplicates
  was written a second time by the batch settle, to delete duplicates its
  plan had already deleted. That is an empty changeset, which writes no
  entry but still queues a capture on the lane.

## 6. Design

### 6.1 One running eval per job

A claim skips every eval whose job has a claimed eval still running on
this manager. The filter is in the claim's SQL (`job_id NOT IN busy`), so a
backlog of thousands of evals for one job never eats the claim's `LIMIT`
and starves the jobs behind it. The busy set is recorded at the claim and
read before the claim adds to it, so one worker may take several evals of
one free job in one batch: it runs them one after another.

When the running handler returns, the next claim takes the oldest waiting
eval and the existing subsumption folds every duplicate into it. Its plan
places everything that was created while the previous plan ran, and its
fused settle deletes every folded eval, in one entry.

### 6.2 When a job stops being busy

- **When its handler returns** (`resolve_claimed_eval` calls
  `Store::eval_handler_returned`), not when the worker's whole batch
  settles. A batch can hold 16 evals, and holding a job busy behind 15
  other jobs' handlers would add their time to its latency for nothing.
- **Except for an eval that blocked.** Its job stays busy until the
  batch's settle writes the `blocked` row. Otherwise a sibling eval could
  run, place the job, clear its blocked rows, and then have the late
  `blocked` write land after it, leaving a job that reads `blocked` with
  nothing blocked.
- **On release and on leadership loss.** `release_eval_claims` and
  `clear_eval_claims` free whatever they release.

### 6.3 Waking workers

A parked worker computes its sleep from the next pending eval. Busy jobs'
evals are left out of that computation, or a worker would wake, claim
nothing, and spin. When a job stops being busy and still has evals waiting
(ready or delayed), one parked worker is woken. Usually the worker that
freed the job claims them itself on its next round. The wake covers that
worker moving on to other work first, at the cost of at most one empty
claim.

### 6.4 Settled evals write nothing more

The batch settle skips evals their plan already settled. Their duplicates
were deleted by the plan (the fused settle carries the same claim's
subsumed list). This removes a capture per collapsed burst.

### 6.5 The fused settle in every placing handler

| Handler | Settles with its plan when |
| --- | --- |
| `job_reschedule` | no batch follow-up is queued after it (unchanged) |
| `job_submit` | the job has no previous config and no batch follow-up |
| `job_update` | no batch follow-up (its removals and resets precede the plan) |
| `job_scale` | it retires nothing (a scale-in queues a capacity pass after its plan) |

**A plan that leaves work unplaced never settles its eval**
(`disarm_eval_settle`, called by the planner before it applies a partial
plan). After a partial plan the handler may write again: a second pass
with the avoided node allowed, or a delayed retry for preemption victims
that are too young. Settling first would lose that follow-up if the
manager died in between, because nothing would run the eval again. This
also closes the same gap in `job_reschedule`, which had it before.

### 6.6 Surface

Nothing new to type for a job author. For the operator:

```nickel
{
  schedulerSettings = {
    # Default true. `false` claims a job's evals in parallel again, as
    # before, so they race at the job-config epoch fence. For comparison
    # and diagnosis only.
    one_eval_per_job = true,
  },
}
```

and one counter, `legion_eval_subsumed_total`: pending evals a claim folded
into the one it took.

## 7. Config, metrics, persistence

- `schedulerSettings.one_eval_per_job` (bool, default `true`), read per
  claim. Documented in `docs/scheduler-and-eval-loop.md` and
  `configs/manager.ncl`.
- `legion_eval_subsumed_total` (counter), in `docs/metrics.md`.
- No proto, jobspec, or persistence change. Claims stay leader memory.

## 8. Phases

1. **One running eval per job** (§6.1 to §6.4). Shipped.
2. **The fused settle in every placing handler** (§6.5). Shipped.
3. **Settle covered evals inside the running plan** (deferred). An eval
   that queued before a running plan read the job's state is already
   satisfied by that plan, yet today it waits, is claimed, plans nothing,
   and settles in an entry of its own. Recording the eval queue's high
   water mark when the handler starts, and deleting same-scope evals below
   it in the plan's fused settle, would remove that entry. It needs the
   mark read strictly before the plan's capacity read, and only pays when
   creates land in the short window between a claim and its snapshot.
   Measure first (§12): the A/B shows how many no-op handlers a burst runs.

## 9. Testing

- **Unit** (`src/storage/store/eval_claims.rs`): a busy job is skipped while
  other jobs are claimed, a parked worker does not see a busy job's evals as
  ready, and the next claim after the handler returns subsumes the whole
  burst. A blocked eval keeps its job busy until released, one batch may
  take several evals of a free job, and the setting off restores parallel
  claims.
- **Integration, burst** (`tests/eval_burst_one_job.rs`): a 48-create burst
  for one job, paced 2 ms apart, pumped by four real eval workers on a
  bootstrapped raft store. Asserts every instance placed, zero rejected
  plans, and fewer plan entries than placements. Its `#[ignore]`d A/B runs
  the burst with the setting off and on at three arrival gaps.
- **Integration, entries** (`tests/raft_job_eval_ops.rs`): exact entries
  for a submit's, an update's, and a no-op reschedule's eval, on both
  topologies.
- **Regression**: `tests/raft_lifecycle_ops.rs` (idle keyed lifecycle,
  unchanged at 8), `tests/blocked_evals.rs`, `tests/eval_loop.rs`,
  `tests/plan_contention.rs`, and the full suite.
- **Load**: `legion-loadgen --mode instances` against a release manager,
  before and after, with the lane metrics (§12).

## 10. Rollout

Default on. A new cluster wants it, and nothing about it is visible except
fewer rejected plans and fewer entries. The setting is the escape hatch.
No upgrade step: claims are memory, so a mixed-version manager set changes
nothing on disk.

## 11. Open questions

- **Should a wait window exist?** Lean no. Batches already grow with load
  because the running plan is the window. A timer would add latency to the
  idle case to buy entries the lane does not need when idle.
- **Should a plan cap its placements?** Lean no, for now. A plan carries
  what queued during one handler cycle, so its size follows the arrival
  rate. `CreateInstances` already places a whole batch in one plan, and
  job submits place full counts. Revisit if a measured burst makes one
  entry large enough to stall the lane.

## 12. Results

Pending: filled in from the runs in the testing plan.
