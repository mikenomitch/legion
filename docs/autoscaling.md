# Scaling Jobs

`legion job scale` changes how many copies of a task run **without creating a
new job version and without starting a rollout**. It is the first half of the
autoscaler ([the plan](../internal_docs/plans/autoscaler-plan.md) tracks the
rest — usage collection, the `scaling` policy block, and node autoscaling are
not built yet).

## Why it exists

Editing `count` in a jobspec and resubmitting is a *deploy*: it mints a new
job config version, cancels any in-flight rollout, and starts a fresh one. An
operator who wants two more replicas during a traffic spike shouldn't get
version-history churn and a deployment. `job scale` writes a **count
override** — a separate record layered over the spec — and reconciles the
running allocation set to it.

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
  task (the common case). With several, the error names the candidates.
- `--reason` is recorded on the override and in the event stream.
- `--json` / `-j` emits the standard envelope; `--idempotency-key` makes the
  RPC retry-safe (`deduplicated: true` on replay).
- Everything is also available over gRPC (`ScaleTask`) and MCP
  (`legion_job_scale`, write-gated).

`legion job scale <name>` (no `--count`) shows the effective picture per
task: `web/default/web: count 6 (spec 2, scaled by operator)`.
(`legion job info` does not carry counts yet — the scale-status surface is
the authoritative view.)

## Semantics

**Effective desired count** = the override if one exists, else the spec's
`count.num`. Every consumer of desired count — the scheduler's plan paths,
slot-mode stateful retirement, rollout group plans — reads the effective
value.

- **Scale-out** enqueues a `job_scale` eval that plans in reschedule mode:
  only the shortfall is placed; existing allocations are untouched.
- **Scale-in** retires surplus allocations: highest slot indices first for
  slot-mode stateful tasks (honoring `stateful.storage.retainOnScaleIn`),
  newest first for stateless tasks.
- **Keyed stateful tasks** are rejected — keyed tasks have no count; use the
  instance API. Tasks with `count: { per: ... }` are rejected in v1.

### Ownership: overrides vs resubmits

An override records the spec count it was laid over (`base_count`). On
resubmit:

- New spec count **equals** `base_count` → the user didn't touch count; the
  override survives the deploy.
- New spec count **differs** → the spec expresses new intent; the override is
  cleared and the spec wins. The submit reply says so.

This makes both stories true: "my scale sticks across deploys" and "editing
the spec does what it says." (This is the two-line version of Kubernetes'
scale-subresource field ownership; Nomad's silent count-reset-on-resubmit is
the failure mode it avoids.)

### Rollout interaction

The ECS rule: **scale-out applies during a rollout, scale-in defers.**
Rollout ticks recompute their targets from the effective count, so a
scale-out mid-rollout grows the step targets on the next tick. A scale-in
during an active rollout is recorded but applied only when the rollout
completes or is cancelled:

```
$ legion job scale web --count 2
deferred: rollout web-7 is in progress; scale-in to 2 applies when it completes
```

A resubmit that only changes `count` (with no override involved) still
creates a new version and a rollout, exactly as before — spec changes make
versions; `job scale` doesn't.

## Access control

Scaling is its own ACL resource: **`scale`** (actions `read`/`write`, scoped
by job name). `job:write` does not imply `scale:write` and vice versa — the
point is granting an autoscaler or a teammate the right to *resize* without
the right to *redeploy*. See [auth-and-acls.md](auth-and-acls.md).

## Observability

- Events: `scale.applied`, `scale.deferred`, and `scale.reset` in the event
  stream ([event-stream.md](event-stream.md)), carrying previous/new counts,
  source, and reason. (`scale.clamped` arrives with the policy block's
  min/max in a later phase.)
- The override row itself (principal, reason, timestamp) is the durable
  record — the event journal is capped and best-effort.
- `legion job scale <name>` with no `--count` prints the current override,
  spec count, and effective count.

## Failure modes

- **Unknown job / task** → `not_found` / `invalid_argument` naming the
  candidates.
- **Keyed or `count.per` task** → `invalid_argument` with the reason.
- **No capacity for the new count** → the `job_scale` eval places what fits
  and the remainder follows the normal reschedule/retry machinery; the
  override stays and later `node_join` evals converge it.
- **Manager not the scheduler-lane leader** → the standard retryable
  `failed_precondition` envelope with the leader hint.

## Known limitations (v1)

- No `scaling` policy block yet — this is the manual surface; automatic
  scaling is the plan's later phases.
- No pause/pin semantics (`--reset` is the only way back to spec ownership).
- `count.per` tasks cannot be scaled.
- Scale status is computed from store state; there is no evaluator status
  RPC yet (that arrives with the autoscaler MVP).
