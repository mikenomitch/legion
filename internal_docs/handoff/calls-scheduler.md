# Judgement calls: scheduler track (spot-check P0-4, P1-2, P1-3, P1-8)

Entries are ranked by project impact, highest first; the number is the rank.

## 1. Dependencies are enforced by the manager holding tasks, not by worker-side gating

`dependency_satisfied` (src/manager/scheduler.rs) no longer counts a batch
dependency that is merely *planned in the same pass* as `complete`/`success`.
Under the new `DependencyGate::Live` gate only a status a worker reported
(`task_status`, written by `Store::complete_allocation`) satisfies them. A
dependent task is **held**: not placed, reported on `PlanOutcome.held_tasks`,
and placed by an eval that `Store::queue_dependency_progress` queues when the
dependency's terminal report lands (`handle_terminated_allocations`).

- **Alternative:** gate starts on the worker (place the whole DAG at once, and
  have the worker wait for its dependencies' status before starting a task),
  which the findings also offered.
- **Trade-off:** each DAG step costs one extra eval and plan (one per terminal
  report of a task that has dependents), and a step starts one heartbeat plus
  one eval after its dependency exits instead of immediately. Resources for a
  held step are not reserved, so a later step can find its node full.
- **Why:** simplicity. The worker never sees the jobspec and has no view of
  other nodes' tasks; worker gating would need a cross-node status feed and a
  new "placed but waiting" allocation state that every surface (UI, rollouts,
  health gates, capacity accounting) would have to learn. Holding in the
  planner reuses the existing readiness filter and the existing eval kinds.
  Surface: `eval.completed` reads `Job pipeline submitted with 1 placements;
  waiting on dependencies: validate, serve`.

## 2. A plan places what fits and reports the rest as unplaced

`build_plan` no longer fails the whole plan when one replica has no node. For
a task outside a multi-task group, the remaining replicas are recorded as
`UnplacedTask { task_name, count, reason, static_port }`, the rest of the plan
applies, and the eval succeeds with `...; 1 unplaced (Failed to place task web:
static port 18080 is already in use on every node it is allowed on ...)`. A
plan that places nothing still fails, now with a typed `PlacementBlocked`
error.

- **Alternative:** keep all-or-nothing and only fix the static-port
  accounting, so `count = 2` with a static port on one node fails cleanly
  (no hot loop) but places nothing.
- **Trade-off:** a job can now run below its count with only an eval outcome
  and a `warn` log saying so; the remainder waits for the capacity pass
  (a node joining, or freed capacity: entry 7). Multi-task groups
  and `job plan` previews stay all-or-nothing, so there are two behaviors to
  explain. Preemption now also runs for a plan that ends up partial: a
  high-band job that can get one of two replicas by evicting one low-band
  allocation now does so (the `disruption.minAvailable` floor still binds),
  where before the whole plan failed and nothing was evicted.
- **Why:** the finding asked for it, and it is what Nomad and Kubernetes do:
  one replica that cannot fit should not keep the other from running.
  Groups stay all-or-nothing because splitting a co-placed group would start
  a sidecar without its primary; previews stay all-or-nothing so `job plan`
  keeps reporting the first replica that would not fit as an error.

## 3. A finished allocation keeps its row but stops holding capacity

A `complete` or `failed` allocation (its process exited) is left out of every
usage sum: the read model's free pools and usage memo, the plan-time ledger
and port view, and the plan-apply transaction's pool, cached usage, and port
check (`resources::capacity_rows`, `status NOT IN ('complete', 'failed')` in
the apply's SQL). Completing an allocation now bumps its node's epoch.

- **Alternative:** delete the row when a batch task finishes (it frees
  everything with no new rule), or zero out its `resources`.
- **Trade-off:** one more rule every capacity reader has to apply (eight
  call sites today), and a completion now fences in-flight plans on that
  node (it bumps the node epoch, so a racing plan conflicts and retries once).
- **Why:** the row is what keeps a finished batch task from running again
  (its slot stays spent, and `job_configs_waiting_on_capacity` and the
  desired-count math read it), so deleting it would reintroduce the re-run
  loop `complete_allocation` was written to prevent. Zeroing `resources`
  would lose what the allocation used from every read surface. Bumping the
  epoch reuses the existing contract ("every capacity-bearing change bumps
  the node") instead of adding a status component to the
  `node_alloc_usage` fingerprint, which would have to read row bodies on
  the apply's hot path. Surface: a 3-step DAG whose steps each need 6 GiB
  runs on one 8 GiB node.

## 4. Job status is derived at read time; the stored column is untouched

`GetJobInfo`/`ListJobs` replace the stored `done` with `running` / `pending` /
`complete` / `failed` computed from live allocations and the latest config's
task statuses (`Store::derived_job_statuses`, `Store::resolve_job_statuses`).
`stopped` and `failed` pass through. The `job.status` column still holds
`done`/`stopped`/`failed`.

- **Alternative:** maintain a real status column (write `running`/`pending`/
  `complete` from every placement, removal, and terminal report), or rename
  the stored `done` to `active`.
- **Trade-off:** two meanings of "status": `legion state query` shows the
  stored flag, the API the derived value. A `ListJobs` call pays a few
  correlated subqueries per active job.
- **Why:** simplicity and correctness. The status moves on events from three
  raft lanes (scheduler: allocations, status: task statuses, control: the job
  row); a stored column would need a cross-lane write on every allocation
  change and could drift. Deriving reads one consistent snapshot. Renaming
  the stored value would have broken every existing comparison (`!= "stopped"`
  checks, KV recovery tests) for no user-visible gain.

## 5. `complete` means "finished either way"; `success` means exit 0

`complete` is now met by a dependency that succeeded *or* failed for good;
before, it excluded `Failed` and so behaved exactly like `success` (the
`Complete` task status is never written).

- **Alternative:** keep `complete` excluding failures (the previous code, and
  one reading of jobs-tasks-and-scheduling.md's "complete or successful").
- **Trade-off:** a behavior change for any spec that used `complete` to mean
  `success`: such a dependent now starts after a failed dependency.
- **Why:** docs/task-supervision.md already documented `complete` as
  "migrate finished, either way", and with the old semantics the two
  conditions were synonyms, so `complete` had no purpose (Airflow's
  `all_done` vs `all_success` is the familiar split). Surface:
  `{ task = "migrate", condition = "complete" }  # runs after migrate, exit 0 or not`.

## 6. A reschedule avoids the failed node with a two-pass plan, not a score penalty

`Eval::schedule_job_allocations` plans a node-carrying `job_reschedule` first
without the failed node; whatever that pass cannot place (an error, or
`unplaced`) is planned again with the node allowed.

- **Alternative:** a scoring penalty for the failed node inside the
  scheduler's policies (one pass).
- **Trade-off:** on a cluster where the failed node is the only fit, the eval
  plans twice. The first pass's failure is logged at `info`, not surfaced.
- **Why:** simplicity and extensibility. A penalty would have to be threaded
  through `bin_pack`, `spread`, and every scheduler plugin's scoring context,
  and a big enough weight is just an exclusion in disguise. Two passes give the
  exact semantics ("anywhere else first, this node only as a last resort")
  with no policy changes.

## 7. Freed capacity retries waiting jobs through the existing capacity pass, gated by an in-memory flag

When an allocation finishes or is removed, a job stops, or a job scales in,
`Store::request_capacity_reconcile_if_waiting` queues the existing cluster
capacity pass (a `node_join` eval with no node), but only while a plan since
the last pass left replicas unplaced (`capacity_waiters`, an `AtomicBool` on
the scheduler leader that starts set and is cleared at the start of each
pass). The pass now also considers unfinished batch tasks.

- **Alternative:** Nomad-style blocked evals: persist each blocked eval with
  the resource classes it needs, and unblock only the evals that freed
  capacity could satisfy.
- **Trade-off:** a free re-plans every job short of its count, not only the
  ones the freed room could help; the flag is per process, so the first free
  after a restart or failover always runs one pass; a job with an eval
  already queued (including the 5 s retry after a plan that placed nothing)
  is skipped by the pass and waits for that eval.
- **Why:** simplicity. It reuses the deduped pass that node joins already
  queue, adds no table and no eval kind, and costs nothing when nobody is
  waiting (no eval, no event). Precise unblocking is the upgrade if passes
  show up in eval-loop profiles.

## 8. A static-port rollout that cannot surge stops the old allocation (destructive update)

When the new version's replacement is a `PlacementBlocked` with a static port
held by old allocations of the same task, `try_place_for_group` stops up to
that many old holders and places on the next tick:
`Group web cannot surge: static port 8080 is held by the version being
replaced; stopped 1 old allocation(s) before placing the new one`.

- **Alternative:** reject `createFirst` for static-port tasks at submit, or
  keep waiting and rely on `capacityBlockTimeout`.
- **Trade-off:** the step is briefly down (old stopped before the new is
  healthy), and the new process may start while the old is still in its
  graceful stop and fail to bind once (its `restart` policy covers it). Only
  the port that blocked is considered; a port held by another job remains a
  plain capacity block.
- **Why:** the finding asked for it; a submit-time rejection cannot know the
  cluster shape (with enough nodes, `createFirst` does work for a static
  port), and waiting is exactly the reported failure.

## 9. Blocked rollouts back off instead of suppressing eval events

A capacity-blocked group re-ticks after `clamp(blocked_for / 2, 2s, 30s)`
(never past `capacityBlockTimeout`), and a dependency-held group after 15s.

- **Alternative:** stop emitting `eval.enqueued`/`eval.completed` for
  `rollout_tick` evals that changed nothing.
- **Trade-off:** a blocked rollout still produces some eval events (about two
  every 30s after the first minute), and capacity that appears is noticed up
  to 30s late.
- **Why:** event emission is generic eval-loop code (src/manager/daemon.rs,
  another track's file) and a per-kind exception there is a special case
  every reader of the stream would have to know. The rollout's own event log
  was already change-gated.

## 10. The planner mirrors the apply's port view, including a Fresh plan's dropped rows

The planner's `PortLedger` counts ports held by the rows a Fresh plan dropped
from its capacity snapshot (the job's own previous allocations), because the
apply transaction still sees those rows.

- **Alternative:** ignore them, like capacity does, and change the apply to
  ignore them too.
- **Trade-off:** a Fresh resubmit of a static-port job whose old allocations
  still exist cannot reuse the port on the same node in one plan (it fails
  with a clear static-port error). In practice `job_update` deletes first and
  rollouts use Reschedule plans, so this only affects the rare
  `job_submit`-with-a-previous-config path.
- **Why:** the planner and apply must agree, or the plan is rejected and the
  eval loops; changing the apply would have allowed two rows holding the same
  static port on one node.

## 11. Unchanged resubmit restarts the job when nothing of it is running

`RunJob` with an unchanged spec queues a `job_update` when the job was
stopped, had failed, or is idle (no live allocation and no placing eval due
now), with a message saying so.

- **Alternative:** restart only stopped/failed jobs, and keep "no update
  needed" for a finished batch job or a pending one.
- **Trade-off:** re-running a finished batch job's spec runs it again, and a
  resubmit of a pending job retries placement immediately.
- **Why:** "run" on something not running should run it; the old message gave
  no way to retry besides `--force-reschedule`, which the user had to know.
