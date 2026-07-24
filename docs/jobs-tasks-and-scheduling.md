# Jobs, Tasks, and Scheduling Semantics

This document describes how jobs are represented and how scheduling behavior works, including edge cases.

## Job Spec Shapes

`JobConfig` supports:

- `task` (single-task shorthand)
- `tasks` as list
- `tasks` as map

Task names:

- map keys become task names
- list tasks default to `task-<n>` unless explicit `name`

## Task Fields That Affect Placement

Important `TaskConfig` fields:

- `count`:
  - integer count
  - or `{ num, per }` for per-attribute placement
- `group`: task grouping for rollout + automatic anti-affinity behavior
- `kind`: task-kind plugin (`exec`, `container`, etc)
- `resources`: normalized by manager `ResourcePolicy`
- `constraints`: hard placement filter
- `permit`: hard node restriction policy
- `affinities`: soft scoring preferences
- `dependsOn`: task dependency graph
- `lifecycle`: `service`, `batch`, `sidecar`
- `schedule`: only valid for `batch`
- `checks` and `on_unhealthy`: health and restart/reschedule policy
- `stateful`: opt-in stable instance identity for the task's replicas. In
  `slot` mode each replica gets an ordinal slot `0..count-1` whose instance id
  and storage handle survive reschedules; replacements reuse the same slot.
  See [Stateful Workloads](stateful-workloads.md) for the full job-spec
  reference, env vars, and scale-in/out semantics. Tasks without a `stateful`
  block behave exactly as before.
- `kv` / `env` / `files` (KV injections): KV-backed env vars and rendered
  files with per-reference `watch`/`onChange`/`onDelete` policies that can
  restart, roll out, or fail the job when watched keys change or are
  deleted. Full semantics (policy precedence, debounce, fail/recovery,
  per-kind capability matrix) live in the
  ["Reactive updates" section of the KV store doc](kv-store.md#reactive-updates-kv-driven-task-updates).
- `identities`: named workload identities (OIDC). The manager mints a
  short-lived, asymmetrically-signed JWT per allocation for each declared
  identity, derived from real scheduling facts, for federating with external
  systems (AWS `AssumeRoleWithWebIdentity`, GCP Workload Identity Federation,
  Vault JWT auth). Each identity has its own audience, TTL, delivery (env
  and/or file), and change mode (`noop`/`signal`/`restart`). See
  [Workload Identity](workload-identity.md) for the full spec reference,
  claim semantics, and provider cookbook. Tasks without `identities` behave
  exactly as before.

## Task Lists and Dependency Readiness

Scheduling uses dependency-aware readiness:

- DAG is topologically sorted.
- Cycles are rejected.
- Dependencies evaluate against current task statuses and already-planned tasks.

Dependency conditions:

- `start`: dependency has started or is being placed now.
- `complete`: dependency is complete/success (or batch task is being placed now).
- `success`: dependency must not be failed and must be success (or batch being placed now).

Only ready tasks enter the current planning pass.

## Constraints, Permits, and Affinities

### Constraints

Hard filters from `ConstraintSet`.
Operators include:

- equality and numeric comparisons
- regex match
- semver match
- contains / contains_any
- is_set / is_not_set
- `distinct` (avoid reusing seen values for a key)

Node attribute lookup order:

1. `config_attrs`
2. `fingerprint_attrs`
3. `dynamic_attrs`

### Permit

Hard allowlist model tied to node restrictions.
If `pool` is not specified, permit defaults to `pool=default`.

Node restrictions are read from:

- implicit `pool`
- optional `config_attrs.restriction` map

### Affinities

Soft score contribution.
Includes regular affinity rules plus `same_node_as` task colocation preference.

## Automatic Anti-Affinity (Subtle Behavior)

Beyond explicit affinity rules, policy scoring applies automatic anti-affinity penalty.

Penalty per candidate node is:

- `max(same_task_allocations_on_node, same_group_allocations_on_node)`

Then weighted by scheduler setting `automatic_anti_affinity` (default `0.2`).

Net effect:

- tasks are biased away from stacking identical task/group allocations on one node,
- but this is soft scoring, not a hard prohibition.

## count.per Behavior

When `count.per` is set:

- eligible nodes are grouped by that attribute token,
- scheduler targets `count.num` per group,
- in reschedule mode it subtracts already-placed count for the same task and group.

If attribute is missing on all candidates, placement fails.

## Group Co-Placement Behavior

When a job group contains multiple tasks:

- planner builds group-level candidate sets from combined constraints,
- assignments may be pinned so tasks in same group/token land on same node,
- conflicting historical placements in same group are treated as errors.

## Node Add/Loss and Reconciliation

## Node Added

Two paths exist:

- Runtime worker registration inserts node directly (`worker_register`), no eval enqueued.
- `node_join` eval path (used in eval-driven flows/tests) marks/inserts node and reconciles count-per/incomplete rollout allocations.

## Node Lost

- Manager heartbeat checker marks stale nodes as lost.
- Lost-node allocations are removed and affected job configs reconciled.
- `node_drop` eval path does equivalent reconciliation.

## Dynamic Node Attr Update

`update_node_dynamic`:

- patches node dynamic attrs
- finds jobs allocated on that node
- enqueues `job_update` evals for those jobs

## Health Checks, Restarting, and Rescheduling

### Worker-side health loop

For each allocation with checks:

- runs boot/readiness/health checks on intervals
- tracks failure windows (`unhealthy_after_count` and optional timespan)

When unhealthy threshold is reached:

1. Try local restart if allowed by `on_unhealthy.restart_count/restart_period/restart_delay`.
2. If restart cannot continue, record allocation failure.

### Manager-side move logic

Worker reports failed allocations in heartbeat.
Manager then:

1. removes failed allocation row
2. checks move history per `(job_config_id, task_config_id)`
3. if `move_count` exceeded in `move_period` -> mark job failed
4. else enqueue delayed `job_reschedule` after `move_delay`

This gives a two-tier model:

- local restarts first,
- then cross-node reschedule,
- then eventual job failure if move budget is exhausted.

## Batch Scheduling Notes

Batch tasks can set `schedule` (`every` or cron). After submit/update/reschedule:

- scheduler computes next run delay for batch tasks,
- enqueues delayed `job_update` follow-up eval for next run.

`task.schedule` is rejected for non-batch lifecycle.

