# Judgement calls: More Placements per Plan Entry

Plan: [`plans/multi-eval-plans-plan.md`](../plans/multi-eval-plans-plan.md).
Entries are ranked by project impact, highest first. An entry's number is
its rank.

## 1. A job's evals are serialized at the claim, and the existing subsumption does the batching

**Call.** While one of a job's evals runs on this manager, a claim skips the
job's other evals (`schedulerSettings.one_eval_per_job`, default on). The
first claim after the handler returns takes the oldest waiting eval, and the
claim's existing duplicate collapse folds the rest into it, so one plan
places what all of them asked for and its fused settle deletes them all.

```nickel
schedulerSettings = { one_eval_per_job = true }   # the default
```

**Alternatives.**
- (a) A worker claims every ready eval of one job together and builds one
  plan for the group, as the TODO item framed it ("let one eval worker claim
  them together"). This is what the claim's subsumption already does for
  evals queued at claim time. It never fired under load because a second
  idle worker claimed each new eval the moment it was queued, while the
  first eval of the job was still running.
- (b) Keep parallel claims and make same-job plans cheaper to lose:
  late-binding already absorbs node-epoch races, so extend it to the
  job-config epoch.
- (c) A short wait window before each claim, so evals pile up and get
  folded.

**Trade-off.** A deliberate exception to strict queue order: an eval of a
busy job waits one handler's duration while lower-priority evals of other
jobs run. Exclusivity is per manager, so during a leadership change two
managers can still race at the epoch fence, as before. A rollout's old and
new configs, which are different epochs, now serialize too (entry 5).

**Why (performance, then simplicity).** Two plans for one job config can
never both commit: the job-config epoch is "a hard, all-or-nothing check"
that candidate lists deliberately do not weaken, because it is what stops
two plans both adding a job's missing replicas. So parallel claims bought
only waste: a rejected plan, which still commits a `rejected` submission row
as an entry, a 25 to 60 ms backoff, and a replan. (b) would weaken that one
check for every count-based job to help keyed ones. (c) charges latency to
the idle case, which is the case users feel. Serializing at the claim needs no
new state on disk and no new concept: it is the `client-go` workqueue and
Nomad eval-broker contract, and the running plan is the batching window, so
batches grow exactly with load. It also fixes a fairness bug for free: one
job's burst can no longer occupy every eval worker.

## 2. A job is free again when its handler returns, except after a block

**Call.** `resolve_claimed_eval` frees the job as soon as the handler
returns, not when the worker's whole batch settles. An eval whose
completion is `Blocked` keeps its job busy until the batch settle writes the
`blocked` row.

**Alternatives.** Free every job at the batch settle, with the claims
themselves. That is one code path instead of two and has no special case.

**Trade-off.** A second release point, and a special case for blocked evals
that a reader has to understand.

**Why (performance).** A batch holds up to 16 evals. Freeing at the settle
would hold a job busy behind up to 15 other jobs' handlers, which is pure
latency for a job whose own handler finished long ago, and it would grow
with exactly the multi-job load the change is meant to keep fair. The
blocked exception exists because a sibling eval running in that gap could
place the job and clear its blocked rows before the late `blocked` write
lands, leaving `job status` reading `blocked` for a job with nothing
blocked. The other outcomes have no such ordering hazard: a settled eval's
write already landed, and a released or finished one does not touch the
job's state.

## 3. A plan that leaves work unplaced never settles its eval

**Call.** The planner calls `disarm_eval_settle` before applying a partial
plan, so the eval settles in its own write after whatever the handler does
next.

**Alternatives.** Keep settling partial plans, as `job_reschedule` did. It
saves an entry on every partial plan.

**Trade-off.** A partial plan costs one more entry than before.

**Why (correctness over a rare entry).** After a partial plan the handler
may still write: a reschedule that avoided a failed node plans the
remainder with that node allowed, and a plan waiting on preemption victims
queues a delayed retry. If the plan had already deleted the eval and the
manager died before those writes, nothing would run the eval again and the
remainder would wait for an unrelated capacity event. That gap already
existed in `job_reschedule`. Arming three more handlers (entry 4) without
closing it would have widened it. Partial plans happen at the edge of
capacity, where one entry is not what limits anything.

## 4. Submit, update, and scale-out settle with their plans, but a resubmit and a scale-in do not

**Call.** `job_submit` arms the fused settle for a job's first config,
`job_update` always (its removals precede its plan), and `job_scale` when it
retires nothing. Each still skips arming when a batch follow-up is queued
after the plan.

**Alternatives.** Arm a resubmit and a scale-in too, by moving their
trailing writes (the previous config's removal, the capacity pass) before
the plan.

**Trade-off.** Arming conditions in four handlers instead of one, and a
resubmit or scale-in still pays the separate settle.

**Why (simplicity).** Reordering a resubmit's removal ahead of its plan
changes what a failed resubmit leaves behind (the old version gone, the new
not placed), which is a behavior change, not an entry trim. These evals are
per job, not per sandbox, so they do not move the keyed lifecycle ceiling.
They are armed because the change is a guarded one-liner each, measured
exactly by `tests/raft_job_eval_ops.rs`.

## 5. Exclusivity is keyed by job, not by job config

**Call.** The busy key is the eval's job id.

**Alternatives.** Key by job config, which is exactly what the epoch fence
serializes, so a rollout's two configs could still plan in parallel.

**Trade-off.** A rollout tick for the new config and a reschedule of the old
one serialize, though their epochs differ.

**Why (simplicity).** Both move the same job's allocations: a tick retires
old-config allocations while it places new ones, and a stop or update
removes every allocation of the job whatever its config. A job is also the
unit Nomad and `client-go` serialize on, and the unit a reader expects.
Eval kinds without a job (node passes, the capacity pass) are not
serialized at all.

## 6. No wait window, and no cap on placements per plan

**Call.** A claim takes what is queued now. A plan carries every instance
that queued during the previous handler cycle, with no cap.

**Alternatives.** A linger (claim only after N ms or M evals), and a cap on
placements per plan with the remainder re-queued.

**Trade-off.** Under a very large burst, one plan entry can be large, and
the lane holds it for proportionally longer.

**Why (performance at idle, simplicity).** The running plan already is the
window: batches grow with arrival rate times handler time and shrink to one
when idle, with no tuning. A cap would need partial-plan semantics for
keyed placements that do not exist today, while `CreateInstances` and job
submits already place whole batches in one plan. Revisit if a measured burst
makes one entry large enough to stall the lane.

## 7. Evals a running plan already covered still run their own handler

**Call.** An eval queued after its job's eval was claimed but before that
handler read the job's state is satisfied by the running plan, yet it still
waits, is claimed, plans nothing, and settles in its own entry. Plan Phase
3 would delete it inside the running plan using a high-water mark of the
eval queue. It is deferred.

**Alternatives.** Build Phase 3 now.

**Trade-off.** One extra entry per handler cycle whose followers all landed
in that short window.

**Why (simplicity until measured).** The window between a claim and its
plan's state read is about a millisecond, against a plan apply of several,
so most waiting evals arrive after the read and are real work. The mark has
to be read strictly before the plan's capacity read to be correct, which is
a new ordering invariant on the plan path. The burst A/B in the plan reports
how many handlers placed nothing, which is the number that would justify
it.

## 8. A settled eval is not written again by its batch's settle

**Call.** `write_eval_completions` skips `EvalCompletion::Settled` outright.
It used to propose a second write for a settled eval whose claim subsumed
duplicates, to delete duplicates its plan had already deleted.

**Alternatives.** Keep the second write as a belt-and-braces delete.

**Trade-off.** If a future plan path ever settled an eval without carrying
its claim's subsumed list, the duplicates would stay queued and run as
no-ops. They would not be lost work.

**Why (performance).** The second write was an empty changeset, which skips
the raft entry but still queues a capture on the scheduler lane's single
pipeline. With bursts now folded, nearly every settled placement had
subsumed duplicates, so the empty capture would have been paid once per
burst cycle.
