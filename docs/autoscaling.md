# Scaling Jobs

`legion job scale` changes how many copies of a task run. It does not create a
new job version or start a rollout. This is the manual half of autoscaling.
Usage collection, the `scaling` policy block, and node autoscaling are not
built yet.

## Why it exists

Editing `count` in a jobspec and resubmitting is a deploy. It creates a new
job config version, cancels any in-flight rollout, and starts a fresh one. If
you just want two more replicas during a traffic spike, you shouldn't get a
new version and a deployment. `job scale` instead writes a **count override**,
a separate record layered over the spec, and reconciles the running
allocations to match it.

## The surface

```
$ legion job scale web --count 6 --reason "launch traffic"
web/default/web: 4 -> 6 (spec count 2; override active)

$ legion job scale web --count 6          # idempotent re-issue
$ legion job scale web                    # no --count: print scale status
$ legion job scale web --reset            # drop the override, return to spec count
$ legion job scale multi --task api --count 3   # disambiguate multi-task jobs
```

- `--task` / `--group` are optional when the job has exactly one scalable
  task (the common case). If it has several, the error lists the candidates.
- `--reason` is recorded on the override and in the event stream.
- `--json` / `-j` emits the standard envelope. `--idempotency-key` makes the
  RPC retry-safe (`deduplicated = true` on replay).
- The same operation is available over gRPC (`ScaleTask`) and MCP
  (`legion_job_scale`, write-gated). The job page in the UI has a **Scale**
  card with the same per-task view and Scale / Reset controls
  ([UI architecture](ui-architecture.md)).

`legion job scale <name>` (no `--count`) shows the effective count per
task: `web/default/web: count 6 (spec 2, scaled by operator)`.
`legion job info` does not show counts yet, so use the scale-status output as
the source of truth.

## Semantics

The **effective desired count** is the override if one exists, otherwise the
spec's `count.num`. Everything that reads desired count uses the effective
value: the scheduler's plan paths, slot-mode stateful retirement, and rollout
group plans.

- **Scale-out** enqueues a `job_scale` eval that plans in reschedule mode.
  Only the shortfall is placed, and existing allocations are untouched.
- **Scale-in** retires surplus allocations. Slot-mode stateful tasks lose
  their highest slot indices first (honoring
  `stateful.storage.retainOnScaleIn`). Stateless tasks lose their newest
  allocations first.
- **Keyed stateful tasks** are rejected because they have no count. Use the
  instance API instead. Tasks with `count = { per = ... }` are also rejected
  in v1.

### Ownership: overrides vs resubmits

An override records the spec count it was laid over (`base_count`). When you
resubmit the job:

- If the new spec count **equals** `base_count`, you didn't touch count, so
  the override survives the deploy.
- If the new spec count **differs**, the spec expresses new intent. The
  override is cleared and the spec wins. The submit reply tells you this
  happened.

So your scale sticks across deploys, and editing the spec still does what it
says. This is a simplified version of Kubernetes' scale-subresource field
ownership. It avoids Nomad's behavior of silently resetting count on
resubmit.

### Rollout interaction

Legion follows the ECS rule: **scale-out applies during a rollout, scale-in
waits.** Rollout ticks recompute their targets from the effective count, so a
scale-out mid-rollout grows the step targets on the next tick. A scale-in
during an active rollout is recorded but only applied when the rollout
completes or is cancelled:

```
$ legion job scale web --count 2
deferred: rollout web-7 is in progress; scale-in to 2 applies when it completes
```

A resubmit that only changes `count` (with no override involved) still
creates a new version and a rollout. Spec changes make versions, and
`job scale` doesn't.

For the same reason, scaling does **not** change the job's `updated_at`. The
jobs list's "Updated" column only tracks spec submissions and status
transitions (see [jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md)).
Scale activity shows up elsewhere: the `scale.applied`, `scale.deferred`, and
`scale.reset` events, and the override's timestamp in
`legion job scale <name>`.

## Access control

Scaling has its own ACL resource: **`scale`** (actions `read`/`write`, scoped
by job name). `job:write` does not imply `scale = "write"`, and the reverse is
also true. This lets you give an autoscaler or a teammate the right to resize
a job without the right to redeploy it. See [auth-and-acls.md](auth-and-acls.md).

## Observability

- Events: `scale.applied`, `scale.deferred`, and `scale.reset` in the event
  stream ([event-stream.md](event-stream.md)). Each carries the previous and
  new counts, source, and reason. (`scale.clamped` will arrive later, along
  with the policy block's min/max.)
- The override row itself (principal, reason, timestamp) is the durable
  record. The event journal is capped and best-effort.
- `legion job scale <name>` with no `--count` prints the current override,
  spec count, and effective count.
- Allocations retired by a scale-in report the `scale_in` stop cause to the
  worker that ran them. They also carry it in `allocation.removed` event
  metadata ([graceful-stop.md](graceful-stop.md)). A task can use this to
  tell "my replica count shrank" apart from "I am being replaced by a new
  version".

## Failure modes

- **Unknown job / task**: `not_found` / `invalid_argument`, listing the
  candidates.
- **Keyed or `count.per` task**: `invalid_argument` with the reason.
- **No capacity for the new count**: the `job_scale` eval places what fits,
  and the override stays. The rest is placed when capacity appears: a worker
  registering or a node being uncordoned queues a capacity pass that
  reschedules jobs short of their count
  ([jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md#node-added)).
  Capacity freed by other jobs stopping does not trigger it.
- **Manager is not the scheduler-lane leader**: the manager forwards the
  request to the leader and returns its answer. Only when that leader cannot
  be reached does the caller get the standard retryable
  `failed_precondition` envelope with the leader hint.

## Known limitations (v1)

- There is no `scaling` policy block yet. This is the manual surface only, and
  automatic scaling comes later.
- There are no pause/pin semantics. `--reset` is the only way to return
  ownership to the spec.
- `count.per` tasks cannot be scaled.
- Scale status is computed from store state. There is no evaluator status
  RPC yet (it arrives with the autoscaler).
