# Built-In Autoscaler Plan

> **SUPERSEDED (2026-10-19) by [autoscaler-v2-plan.md](autoscaler-v2-plan.md)**
> and its substrate, [legion-apps-plan.md](legion-apps-plan.md). Phase 1 below
> shipped and is still accurate; v2 treats it as its Phase 0. Phases 2-7 are
> replaced. The autoscaler now runs as a Legion app (a job hosted on the
> managers' system workers), not as an in-core loop, and it gains
> JSON-RPC plugins, UI extensions, and a different `autoscale` default. Which
> of this plan's calls stand is tabled in
> the ledger at the end of
> [autoscaler-v2-plan_calls.md](../calls/autoscaler-v2-plan_calls.md#ledger-what-stands-from-the-superseded-plans-calls).
> The text below is kept unedited as history.

**Status (historical): IN EXECUTION.** Phase 1 (`legion job scale` + count overrides —
§6.1/§6.2/§8.1) shipped 2026-07-30 with docs in
[docs/autoscaling.md](../../docs/autoscaling.md); deviations from this plan
are recorded in the [calls doc](../calls/autoscaler-plan_calls.md) (notably:
`task_scale` is control-lane, not scheduler-lane, and deferred scale-ins use
a `pending_count` column promoted on rollout completion). Phases 2-7 are
unstarted. Deliberately deferred from v1:
request-rate/connection-count signals (blocked on the networking data path),
wake-on-request scale-to-zero (blocked on the ingress proxy), autoscaling keyed
stateful instances, dylib/WASM policy plugins, and the generic UI-extension
mechanism (which needs its own plan — see §6.8).

> **Judgement calls:** [autoscaler-plan_calls.md](../calls/autoscaler-plan_calls.md)

---

## 1. Problem statement

Legion has no way to change how many copies of a task run except editing the
jobspec and resubmitting. That single gap causes three distinct pains:

1. **Manual scaling is a deploy.** Changing `count: 2` to `count: 6` creates a
   new job config version (`JobConfig::is_same_as` treats count like any other
   field, `src/structs/job.rs:2006`), cancels any in-flight rollout
   (`cancel_active_rollouts_for_job`, `src/manager/server.rs:1419`), and starts
   a fresh rollout. An operator who just wants two more replicas during a
   traffic spike gets version-history churn and a deployment.
2. **Automatic scaling is impossible.** There is no policy surface, no
   evaluator, and no input signal: Legion collects zero live usage data — the
   worker heartbeat carries allocation IDs and health booleans only
   (`proto/legion.proto:1978-2007`), and the only CPU/memory numbers in the
   state store are a boot-time fingerprint snapshot that never refreshes
   (`src/worker/node_check/node_checkers/kitchen_sink.rs:50,118`).
3. **Cluster capacity is static.** Nodes join when an operator runs
   `legion join` somewhere. Nothing can notice unplaceable work and provision a
   node, or notice an idle node and retire it. Scale-in has no RPC to even
   remove a node record (`Store::remove_node` exists at
   `src/storage/store.rs:2868` but no endpoint calls it).

The TODO already frames the destination: the autoscaler is the first "ecosystem
app" — *"run inside the core binary and be splittable into its own job, so day 1
is simple and day N is performant"* (`internal_docs/TODO.md` § Ecosystem apps,
`internal_docs/differentiators.md:72`). This plan is that app, plus the two
primitives it needs that are independently valuable: a scale surface that
doesn't churn versions, and real usage collection.

## 2. Prior art

### Nomad Autoscaler ([overview](https://developer.hashicorp.com/nomad/tools/autoscaling), [repo](https://github.com/hashicorp/nomad-autoscaler))

A separate daemon, usually deployed as a Nomad job, with a three-way plugin
split that is the cleanest part of the design and the part we copy:

- **APM plugins** (`prometheus`, `nomad-apm`, `datadog`): `Query(query, range)
  -> timeseries`. The query string is opaque, passed verbatim from policy to
  plugin.
- **Strategy plugins** (`target-value`, `threshold`, `pass-through`,
  `fixed-value`): `(check, metrics, current_count) -> desired_count`.
- **Target plugins** (`nomad` task groups, `aws-asg`, `azure-vmss`, `gce-mig`):
  `Status() -> {count, ready}` and `Scale(action)`. On cluster scale-in the
  target plugin picks victims (`least_busy`, `empty`, `newest_create_index`),
  drains them with a deadline, then terminates the specific instances.

What we reject, each with a scar to point at:

- **Scaling registers a new job version.** `Job.Scale` raft-applies a full
  `JobRegisterRequest`, so active autoscaling floods version history.
- **Resubmit resets the count.** Nomad has no field ownership; `nomad job run`
  applies whatever count the spec holds, silently reverting the autoscaler
  ([nomad#9839](https://github.com/hashicorp/nomad/issues/9839), closed without
  a fix). The standard workaround is templating the live count back into the
  spec at deploy time.
- **Separate daemon.** Extra deploy/monitor surface, single-instance until HA
  landed in v0.4.0, and policies split across two homes (jobspec blocks vs HCL
  files on disk that aren't in cluster state).
- **Scale requests are rejected during deployments** (400) after races like
  [nomad#14669](https://github.com/hashicorp/nomad/issues/14669) — safe but
  blunt; you can't add capacity during a deploy.
- **`min = 0` with no activation path.** Nothing wakes a zero-count group;
  Koyeb built a serverless layer around Nomad partly because of this
  ([writeup](https://www.koyeb.com/blog/the-engineering-behind-autoscaling-with-hashicorps-nomad-on-a-global-serverless-platform)).
- **Multi-check merge was a footgun**: "safest wins" meant any healthy check
  vetoed scale-in until check `group`s were bolted on.

### Kubernetes HPA ([docs](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/))

The important idea is the **scale subresource**: desired count is a separate,
separately-owned field, so changing it never touches the pod template, never
creates a new ReplicaSet, never restarts anything. Server-side apply gives the
HPA ownership of `replicas`, so a client re-applying the field gets a conflict
instead of a silent reset. Also worth copying: the algorithm
(`desired = ceil(current × metric/target)` with a ~10% tolerance band),
per-direction damping (`stabilizationWindowSeconds`: 0 up, 300 down, using the
max recommendation over the window), multiple metrics merge as max-of-desired,
and conservative treatment of missing data.

### KEDA ([docs](https://keda.sh/docs/latest/concepts/scaling-deployments/))

Scale-to-zero done right: two thresholds per trigger. `activationThreshold`
governs 0↔1 and is handled by the operator directly; the regular threshold
governs 1→N through the HPA. The 0→1 wake is event-driven (queue depth, cron),
never "CPU of the pods that don't exist." Also: pause annotations as the
explicit deploy-freeze story, and `fallback` replicas after N scaler failures.

### Cluster Autoscaler and Karpenter ([CA FAQ](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md), [Karpenter](https://karpenter.sh/docs/concepts/))

CA scales node groups when pods are *unschedulable* — the trigger is pending
work, not a utilization gauge. Scale-down requires <50% utilization for 10+
minutes *and* every pod evictable elsewhere, then drain-before-terminate.
Karpenter drops node groups entirely and bin-packs pending pods onto exact
instance types. The lesson we take: **scale-up keys on unplaceable work**
(crisp, works from zero nodes), utilization targets are the secondary headroom
signal.

### Simpler baselines

- **Fly.io autostop/autostart**: the proxy is the autoscaler — three config
  fields, wake-on-request. The cleanest scale-to-zero story; ours when the
  ingress proxy exists.
- **ECS target tracking**: missing data never scales in; multiple policies —
  any may scale out, all must agree to scale in; and **scale-in is suspended
  during deployments while scale-out continues**. We adopt that rollout rule
  verbatim (§6.2).

## 3. User scenarios

**S1 — bump the count, no ceremony.** A service is getting hammered. The
operator runs `legion job scale web --count 6 --reason "launch traffic"`.
Six replicas within seconds, no new job version, no rollout, the in-progress
canary (if any) keeps going. `legion job info web` shows `count 6 (spec 2,
scaled)` and a scaling event records who/when/why.

**S2 — set-and-forget CPU scaling.** A user adds four lines to their jobspec
and never thinks about it again:

```jsonnet
scaling: { min: 2, max: 20, target: { cpu: 0.7 } },
```

Legion samples real usage, holds average CPU near 70% of the task's request,
scales out fast, scales in slowly, never fights a deploy, and shows its last
ten decisions in the UI and `legion job scale-status web`.

**S3 — queue worker that sleeps.** A batch-draining service scales on Kafka lag
from the team's existing Prometheus, to zero when the queue is empty:

```jsonnet
scaling: {
  min: 0, max: 30,
  checks: [{
    name: 'lag',
    source: 'prometheus',
    query: 'sum(kafka_consumergroup_lag{group="ingest"})',
    strategy: { name: 'per-replica', value: 500 },
    activation: 1,   // any lag wakes it from zero
  }],
},
```

**S4 — the cluster grows itself.** Ops declares a node group backed by an AWS
ASG. When the scheduler can't place work, Legion computes how many nodes of
that group's shape are needed, tells the provider, and the new workers join
with a pre-minted token (firing the existing `node_join` path, which the
artifact pre-warm plan already hooks for cache warming — its scenario C3).
Overnight, empty nodes are drained, terminated, and forgotten.

## 4. Goals and non-goals

**Goals**

1. `legion job scale` — change a task's count without a new job version, a
   rollout, or a fight with one. Useful with zero autoscaler involvement.
2. Real per-allocation CPU/memory usage collection, visible in the CLI/UI even
   if autoscaling is never enabled.
3. A jobspec `scaling` block whose simple form is one line, evaluated by a
   built-in manager loop that is on by default and costs nothing when unused.
4. Extensibility along Nomad's proven seams — metric **sources**, **strategies**,
   node **providers** — as internal traits from day one, loadable plugins later.
5. Node autoscaling driven primarily by unplaceable work, with drain-based
   scale-in and an exec-ABI provider contract (volume-plugin precedent).
6. The whole evaluator packaged as `legion autoscaler agent`, runnable as a
   normal Legion job with control-plane workload identity — the template every
   later ecosystem app (cert manager, ingress, GitOps) follows.
7. Every decision auditable: scaling events in the event stream, status RPCs,
   UI panels.

**Non-goals (v1)**

- **RPS/connection-count signals.** There is no data path to observe
  (`docs/service-discovery.md` — catalog only; `internal_docs/TODO.md`
  § Networking: "No plan yet"). The check schema reserves the shape
  (`source: 'legion', query: 'service.rps'`); external Prometheus covers the
  gap meanwhile.
- **Wake-on-request scale-to-zero.** Needs the ingress proxy (Fly's model).
  `min: 0` ships, but only external signals (queue depth, schedule) can wake a
  task; validation enforces that honestly (§6.4).
- **Autoscaling keyed stateful instances.** Keyed tasks have no `count`
  (explicitly deferred in the stateful plan). Slot-mode tasks scale normally,
  honoring `retainOnScaleIn`.
- **Vertical autoscaling** (resizing requests). Nomad paywalled it; we skip it.
- **Scaling `count.per` tasks.** `scaling` on a task with `count: { per: ... }`
  is a validation error in v1; the interaction (scale the per-group num?) is
  deferred until someone needs it.
- **A new raft lane.** The multi-raft plan pre-rejects lane-per-subsystem
  (`internal_docs/plans/multi-raft-persistence-plan.md` §"Lanes partition by
  write class"). Scale writes ride the Scheduler lane; policy documents ride
  Control.
- **The generic UI-extension mechanism** (jobs registering panels). It is a
  real security boundary and gets its own plan; this plan ships first-party UI
  plus a deliberately tiny "Apps link" primitive (§6.8).
- **Federation awareness.** Node groups get a region-qualifiable name so the
  axis exists later, nothing more.

## 5. Background — what exists today

Code recon the design leans on (details verified against the tree):

- **Count is per-task.** `TaskConfig.count: TaskCount { num, per }`
  (`src/structs/job.rs:189,1312`). Keyed stateful tasks reject an explicit
  count; desired comes from live instances (`count_live_keyed_instances`).
- **Any spec change is a new version.** `handle_job_submit_apply` compares with
  `JobConfig::is_same_as` (structural equality including count,
  `src/storage/store.rs:4047`, `src/structs/job.rs:2006`) and bumps
  `version`; `job_config_id = "{name}-{version}"`. A resubmit cancels active
  rollouts (`src/manager/server.rs:1419,1482`).
- **There is already a "count is schedule-level, not template-level" seam.**
  `task_schedule_equivalent` (`src/storage/store.rs:10476`) migrates
  allocations across config versions when only non-schedule fields changed —
  count is deliberately in the schedule-relevant set.
- **The scheduler cannot exceed the stored count.** Desired comes from
  `tasks_for_plan_from_store` reading `config.count.num`
  (`src/manager/scheduler.rs:1598`); the rollout's target channel *clamps*
  (`*desired = (*target).min(*desired)`, `scheduler.rs:669`). `Reschedule`
  mode only adds (`required = desired − placed`, `scheduler.rs:819`); scale-in
  happens via `Fresh` re-plans or rollout retirement
  (`retire_old_allocations`, `src/manager/rollout.rs:1357`). Slot scale-in
  reads count straight from SQL by `version DESC`
  (`slot_still_desired_tx`, `src/storage/store.rs:6125`) — every place that
  answers "what is desired?" must learn about overrides (§6.1).
- **No usage anywhere.** Heartbeats carry IDs + health booleans
  (`proto/legion.proto:1978-2007`); `allocation_health` is a last-value table
  written per heartbeat on the Status lane; `/metrics` is manager-only,
  cluster-level, cardinality-capped (`src/manager/telemetry.rs`); no
  time-series retention of any kind; Legion has no Prometheus *client*.
- **The manager background-loop pattern is established.**
  `start_node_status_checker` (`src/manager/daemon.rs:667`) ticks on an
  interval and gates mutations on `Store::is_raft_leader(lane)`
  (`src/storage/store.rs:465`); the leadership-acquisition watcher
  (`daemon.rs:120-158`) re-runs leader-only work after elections; eval workers
  restart on exit. The autoscaler loop copies this shape exactly.
- **The split-out mode is nearly unblocked already.** Control-plane workload
  identity lets a task authenticate to the manager as `legion:<sub>` with
  policies attached via `workloadIdentity.controlPlaneBindings`
  (`docs/workload-identity.md` §control-plane identity,
  `src/manager/auth_method.rs`). Read surface is strong: `GetLedger`,
  `GetNodes`, `ListInstances`, `StreamEvents`, and `QueryStateStore` (raw
  read-only SQL, `proto/legion.proto:569`). KV locks
  (`AcquireKvLock`/`RenewKvLock`, `proto/legion.proto:525-534`) are a
  ready-made leader election for a replicated agent job. Missing: no
  `LEGION_MANAGER_ADDR` in the injected env set.
- **Node lifecycle**: join is token-mediated (`CreateJoinToken`,
  `proto/legion.proto:693` — the natural provisioning hook); drain shipped
  (`SetNodeAvailability`, cordon/drain/uncordon CLI, `node_drain` evals); the
  scheduler filters to `availability == "available"`
  (`src/manager/scheduler.rs:885`). **No node-removal RPC exists.** Node
  `pool` is ACL/config-only; nothing scheduling-side consumes it
  (`internal_docs/TODO.md` § Scheduling: worker groups).
- **Plugin system**: six kinds; five dylib, volumes use an exec ABI (JSON on
  stdin, op as argv, timeout — `src/volumes/exec.rs`) chosen precisely for
  process isolation around untrusted/external work. Dylibs are fully trusted;
  there is no sandboxing; the manager loads rollout/task plugins for
  validation. No registry exists.
- **UI**: SolidJS SPA over gRPC-Web, routes hardcoded in `ui/src/App.tsx`; the
  Observability section shows the capability-probe pattern for conditionally
  visible nav (`App.tsx:105-163`). The UI already computes request-based node
  utilization client-side (`NodeDeepView.tsx` `buildResourceUsage`).
- **ACLs**: 8 closed resources (`job|node|kv|state|operator|acl|exec|action`,
  `src/structs/acl.rs:19-58`), read/write actions, job/pool/path scopes.
  `tests/integration/acl_endpoints.rs` fails the build on any unclassified RPC.

## 6. Proposed design

### 6.1 The scale surface: count overrides

A new store concept: a **count override**, keyed by the version-stable task
tuple `(job_id, group, task_name)` — the same handle `slot_still_desired_tx`
and `count_live_keyed_instances` already use, deliberately not the
version-scoped `task_config.id`.

```
task_scale(
  job_id, task_group, task_name,   -- PK
  count,                            -- the override
  base_count,                       -- spec count when the override was set
  source,                           -- 'operator' | 'autoscaler'
  reason, principal, updated_at
)
```

**Effective desired count** = override if present, else `task_config.count.num`.
One new store accessor (`effective_count_for_task`) is consulted by every
current reader of `count.num`: `tasks_for_plan_from_store`
(`scheduler.rs:1598`), `slot_still_desired_tx` (`store.rs:6125`), rollout
`build_group_plans` (`rollout.rs:1460,1588-1609`), and the keyed/slot staged
adjustments. The rollout clamp at `scheduler.rs:669` needs no change — desired
now reflects the override before the clamp applies.

The user-facing surface:

```
$ legion job scale web --count 6 --reason "launch traffic"
web/default/web: 4 -> 6 (spec count 2; override active)

$ legion job scale web --count 6        # idempotent; also accepts --task/--group
$ legion job scale web                  # no --count: prints scale status
$ legion job scale web --reset          # drop the override, return to spec count
```

Mechanics:

- New RPC `ScaleTask { job_id, task_group, task_name, count, reason,
  idempotency_key }` following the StopJob pattern (idempotency lookup/record,
  standard envelope). Group/task optional when unambiguous (single-task jobs —
  the common case — need only the job name).
- Writes the override on the **Scheduler lane**, then enqueues a new eval kind
  **`job_scale`**: scale-out plans in `Reschedule` mode (adds the shortfall);
  scale-in retires surplus — highest slot indices first for slot tasks
  (matching existing slot semantics and `retainOnScaleIn`), newest allocations
  first for stateless tasks.
- Validation: count within the policy's `[min, max]` if a `scaling` block
  exists (error tells you the clamp); rejected for keyed tasks (same error the
  instance API gives today, inverted) and `count.per` tasks in v1.
- Events: `scale.applied` / `scale.deferred` / `scale.clamped` in the event
  stream, carrying prev/new/source/reason. Because the journal is capped and
  best-effort, the override row itself (with principal + reason + timestamp)
  is the durable record; `legion job scale-status` shows both.
- ACL: new resource **`scale`** (read/write, job-name scope). `job:write` does
  not imply `scale:write` and vice versa — the whole point is granting an
  autoscaler (or a teammate) the right to resize without the right to
  redeploy. Both new RPCs get `tests/integration/acl_endpoints.rs` entries.

### 6.2 Ownership: overrides vs resubmits vs rollouts

The Nomad wart we must not reproduce is the silent count reset on resubmit;
the K8s failure mode on the other side is "editing count in my spec does
nothing and I don't know why." The rules, in order:

1. **If the task has a `scaling` block, the policy owns the count.** The spec's
   `count` is the *initial* value, used at job creation and as the starting
   point when a policy is first attached. Resubmits never reset an override.
   Submitting a spec whose `count` lies outside `[min, max]` is a validation
   error; changing `count` on a resubmit while a policy exists produces a
   warning in the submit reply ("count is autoscaled; spec count ignored").
2. **If there is no `scaling` block, intent is arbitrated by `base_count`.**
   A manual override records the spec count it was laid over. On resubmit:
   - new spec count == `base_count` → the user didn't touch count; the
     override survives.
   - new spec count != `base_count` → the user expressed new intent in the
     spec; the override is cleared and the spec wins. The submit reply says so.
   This is a two-line approximation of server-side-apply field ownership, and
   it makes both stories true: "my scale sticks across deploys" and "editing
   the spec does what it says."
3. **Rollout interaction is the ECS rule: scale-out applies, scale-in defers.**
   Rollout ticks recompute `desired_new` from the effective count, so a
   scale-out mid-rollout takes effect on the next tick (targets and
   `total_steps` grow; a step that was satisfied may briefly become
   unsatisfied — that is the correct behavior, capacity was requested).
   A scale-*in* during an active rollout is recorded but not applied until the
   rollout completes or is cancelled:

   ```
   $ legion job scale web --count 2
   deferred: rollout web-7 is in progress; scale-in to 2 applies when it completes
   ```

   This avoids the Nomad 400 (can't add capacity during a deploy) *and* the
   mid-rollout retirement races that motivated it. The autoscaler obeys the
   same rule automatically since it goes through the same path.

Deliberately *not* changing: a resubmit that only changes `count` (no policy)
still creates a new version and a rollout, exactly as today. One rule — spec
changes make versions, `job scale` doesn't — is easier to explain than a
carve-out, and `legion job scale` is now the recommended path anyway.

### 6.3 Usage collection: the missing input

The long pole, and independently valuable observability. Design:

- **Worker samples per-allocation CPU and memory** on its existing heartbeat
  cadence: cgroup v2 stats on Linux (`cpu.stat`, `memory.current` — the exec
  runtime already knows cgroup paths), process-tree sampling via `sysinfo` as
  the macOS/fallback path, runtime-reported stats where a runtime has them.
  Sampling is best-effort per runtime; a runtime that can't report usage
  reports nothing (and policies over it fail visibly, not silently — §6.5).
- **Samples ride the existing heartbeat write**, as new optional fields on
  `AllocationHealth` (`cpu_used_mhz`, `memory_used_mib`, `sampled_at`). That
  lands them in the `allocation_health` last-value table on the Status lane —
  a wider row on a write that already happens every heartbeat, not a new write
  path, and replicated so any manager (and the split-out agent) can read them.
- **Windows live in the evaluator, not the store.** The autoscaler keeps
  per-task in-memory rings of the samples it reads each tick (HPA-style
  stabilization needs minutes of history, not a time-series database). On
  leader failover the window restarts; the loop refuses to scale *in* until a
  full stabilization window has elapsed since it started — conservative in
  the safe direction. No new tables, no retention policy, no cardinality
  explosion in `/metrics` (per-instance series would break the 256-combo cap
  tenet; aggregates can come later behind a flag).
- **Read surface**: `GetLedger`'s `LedgerAllocation` and `GetJobInfo` gain
  usage fields; `legion node info` / `legion job info` and the node/job UI
  pages show request vs actual. This ships before any autoscaling and is worth
  having alone.

### 6.4 The `scaling` block

Two forms. The simple form is the product: one line for the 90% case.

```jsonnet
// Simple: hold average utilization of the task's *request* at the target.
tasks: [{
  name: 'web',
  count: 2,                       // initial; policy owns it from here
  resources: { cpu: 500, memory: 512 },
  scaling: {
    min: 1,
    max: 20,
    target: { cpu: 0.7 },         // and/or memory: 0.8 — max of desired wins
  },
}]
```

```jsonnet
// Full: explicit checks. The simple form desugars to exactly this.
scaling: {
  min: 0,
  max: 30,
  cooldown: '2m',                     // min gap between actions in the same direction
  scaleDown: { stabilization: '5m' }, // scale-in uses max desired over the window
  headroom: 1,                        // add N replicas on top of whatever checks compute
  checks: [
    {
      name: 'cpu',
      source: 'legion',               // built-in source: cluster state + usage
      query: 'task.cpu.utilization',  // fraction of requested cpu, averaged over replicas
      strategy: { name: 'target-value', target: 0.7 },
    },
    {
      name: 'lag',
      source: 'prometheus',           // built-in source: external HTTP query
      query: 'sum(kafka_consumergroup_lag{group="ingest"})',
      window: '1m',
      strategy: { name: 'per-replica', value: 500 },  // desired = ceil(metric / 500)
      activation: 1,                  // KEDA-style: 0 -> 1 only when metric >= 1
    },
  ],
},
```

Semantics, mostly borrowed and credited:

- **Algorithm** (`target-value`): `desired = ceil(current × metric/target)`,
  10% tolerance band (HPA). `per-replica`: `desired = ceil(metric / value)` —
  covers queue depth and Nomad's `pass-through` niche (an external system
  computing counts writes them to a metric). Later strategies (`threshold`,
  `schedule` for cron windows) slot into the same enum.
- **Merge rule**: compute desired per check, take the max; a check with
  missing data contributes nothing to scale-in decisions and its failure is a
  visible event (ECS: missing data never scales in; HPA: max wins). No
  check-group machinery — max-of-desired doesn't have Nomad's veto deadlock.
- **Damping**: `cooldown` (default 0 up / covered by stabilization down),
  `scaleDown.stabilization` default `'5m'`, scale-out instant by default (HPA
  defaults, the mainstream expectation).
- **`headroom`** answers "target number plus overhead" directly: it is added
  after merge, before clamping. `min`/`max` clamp last, always.
- **`min` defaults to 1.** `min: 0` is allowed only when at least one check
  has an `activation` threshold on a source that exists at zero replicas
  (external, or later a schedule). A policy that only watches its own task's
  usage cannot wake from zero — there is nothing to measure — so validation
  rejects it rather than letting it deadlock:

  ```
  error: scaling.min = 0 requires a check that can wake the task from zero.
    'cpu' reads task usage, which is empty at count 0.
  hint: add an external check with `activation`, or set min >= 1
  ```

- **Defaults for the simple form**: `target.cpu`/`target.memory` are fractions
  of the task's *requested* resources (requests already exist and default
  sanely — `src/resources.rs`), evaluated over a 1m window, averaged across
  replicas.

Policy *sharing* needs no new machinery in v1: policies are data, and jsonnet
already imports. A registry entry that ships
`legion-policies/web-cpu.libsonnet` is immediately consumable as
`scaling: (import 'legion-policies/web-cpu.libsonnet')`. When the registry
plan lands, autoscaling policies are just another thing it distributes. Custom
policy *logic* (a new strategy) is a plugin question — §6.9.

### 6.5 The evaluator

One loop, one code path for both deployment modes:

```
every evaluationInterval (default 15s):
  if !is_raft_leader(Lane::Scheduler): continue          // in-core mode
  for each task with a scaling block (policies indexed from job configs):
    read inputs (allocation_health usage, ledger, external sources)
    desired = clamp(headroom + max(check strategies), min, max)
    if desired == effective_count or within tolerance: continue
    if damped by cooldown/stabilization: record 'held' status, continue
    ScaleTask(source: 'autoscaler', reason: <check that won>)   // same path as the CLI
```

- **Status, not just events.** Per-task evaluator state (last computed desired,
  which check won, why it held, last error per source) is kept in memory and
  served by a `GetJobScaleStatus` RPC — this is what the UI and
  `legion job scale-status web` render. Events record *actions*; status
  answers "why is it not scaling?", which is the question operators actually
  ask (Nomad uses count-less scale events for this; a status RPC is more
  direct).
- **Failure modes are loud.** A source that errors marks its checks `failing`
  in status, emits `scale.check_failed` (throttled), contributes nothing to
  the merge, and — per the merge rule — can therefore never cause scale-in.
  N consecutive failures with `min: 0` never wake or sleep anything.
- **Sources and strategies are internal traits from day one**, mirroring the
  Nomad boundary so the plugin cut in §6.9 is mechanical:

  ```rust
  trait MetricSource {           // 'legion', 'prometheus'
      fn query(&self, q: &str, window: Duration, ctx: &ScaleCtx) -> R<Vec<Sample>>;
  }
  trait ScaleStrategy {          // 'target-value', 'per-replica'
      fn desired(&self, samples: &[Sample], current: u32, cfg: &Value) -> R<Option<u32>>;
  }
  ```

The `legion` source's v1 query vocabulary is deliberately tiny and namespaced
for growth: `task.cpu.utilization`, `task.memory.utilization` (fractions of
request, from §6.3), `task.count.pending` (allocations not yet healthy).
`service.rps` / `service.connections` are *reserved names* documented as
"needs the mesh data path" so the eventual wiring is additive.

### 6.6 Where it runs: in-core by default, a job when you want it elsewhere

**Default: a manager subsystem.** `run_manager` spawns
`start_autoscaler_loop(store, config)` beside the node-status checker, gated
on `autoscaler.enabled` (default `true` — with no scaling blocks and no node
groups the tick is a no-op index check). Mutations gate on
`is_raft_leader(Lane::Scheduler)`; the leadership watcher restarts cleanly
after elections. Simple clusters get autoscaling with zero extra processes,
policies live in cluster state, and there is no HA story to operate — the
three Nomad daemon pains, dissolved by being built in.

**Escape hatch: the same evaluator as a job.** `legion autoscaler agent
--manager <addr>` runs the identical loop out-of-process against the public
API: policies via job specs, usage via `GetLedger`, scaling via `ScaleTask`,
node groups via their status RPC. Replicas coordinate with a KV lock
(`AcquireKvLock` TTL + renew — already shipped). Auth is control-plane
workload identity, no secrets in the spec:

```jsonnet
// The autoscaler, as a job, on the workers.
{
  name: 'legion-autoscaler',
  tasks: [{
    name: 'agent',
    runtime: 'exec',
    config: { command: 'legion', args: ['autoscaler', 'agent'] },
    identities: [{ name: 'legion' }],   // control-plane identity
    count: 2,                           // KV-lock leader election between them
    resources: { cpu: 200, memory: 256 },
  }],
}
```

```jsonnet
// Manager config: hand the loop off and bind the job to a least-privilege policy.
autoscaler: { enabled: false },   // in-core loop off; the job owns scaling now
workloadIdentity: {
  controlPlaneBindings: [
    { claim: 'legion.io/job', value: 'legion-autoscaler', policies: ['autoscaler'] },
  ],
},
// policy 'autoscaler': scale:write on *, job:read, node:read+write, state:read
```

Two small enablers ship with this phase, both generally useful: tasks get
`LEGION_MANAGER_ADDR` injected (the one env var missing for any
manager-talking workload), and the docs name this **the escape-to-job
pattern** — config-gated in-core loop, same code as a subcommand, control-plane
identity, KV-lock leadership — as the template cert manager, ingress, and the
GitOps helper follow. We deliberately do *not* build "jobs pinned to manager
nodes" for this: manager-as-worker already exists (`--run-worker`) for anyone
who wants managers schedulable, and a system-job placement mechanism is its
own future plan; the in-core loop covers the "simple cluster" case better
than a bootstrapping-ordered special job would.

### 6.7 Node autoscaling

Scoped to node *groups*, declared in manager config (cluster state, not files
on disk next to a daemon):

```jsonnet
autoscaler: {
  nodeGroups: [{
    name: 'workers-general',
    workerType: 'default',            // the worker type new nodes register as
    min: 1,
    max: 20,
    provider: {
      name: 'exec',                   // v1 provider ABI; 'aws-asg' later
      command: '/opt/legion/providers/asg.sh',
      config: { asgName: 'legion-workers', region: 'us-east-1' },
    },
    headroom: { cpu: 0.2 },           // optional: keep 20% allocatable free
    scaleIn: {
      selector: 'empty',              // 'empty' | 'least_busy' (request-based)
      after: '10m',                   // sustained before acting
      drainDeadline: '5m',
    },
  }],
}
```

- **Scale-up keys on unplaceable work** (the Karpenter/CA lesson). The
  scheduler already computes `required − placed`; it records per-task
  placement shortfalls (count + the constraint/resource shape that failed).
  The evaluator matches shortfalls against each group's node template (worker
  type attributes + resources — the same attr matching constraints use today)
  and asks the provider for `ceil(shortfall / per-node capacity)` more nodes,
  clamped by `max` and cooldown. This works from zero nodes — no metric
  needed, pending work *is* the signal (Nomad's utilization-query approach
  can't scale from zero). The `headroom` target is the secondary, gauge-based
  signal on allocated-vs-capacity ratios from the existing `ResourceLedger`
  accounting.
- **Scale-up flow**: mint a TTL'd join token (`CreateJoinToken` — already an
  authenticated RPC), invoke the provider's `scale_up` op with count + token +
  worker type; new nodes join normally and fire `node_join` evals — which is
  exactly the hook the artifact pre-warm plan's C3 scenario is waiting on.
- **Scale-in flow**: pick victims by `selector` (`empty` = zero non-terminal
  allocations, the default and the only selector that can't disrupt;
  `least_busy` = lowest requested/capacity), cordon + drain with the shipped
  `SetNodeAvailability` path (stateful `onLost` protection applies), wait for
  drain, call the provider's `terminate` op with the node's provider instance
  ID, then call the new **`ForgetNode`** RPC (the missing removal endpoint;
  `node:write`, also useful standalone for retiring dead records). Instance
  identity comes from a node attribute (`providerInstanceId`) that the
  provisioning template sets at join.
- **The provider contract is an exec ABI, not a dylib** — the volume-plugin
  precedent (`src/volumes/exec.rs`): ops (`status`, `scale_up`, `terminate`)
  as argv, JSON on stdin/stdout, timeout, no cloud SDKs compiled into the
  manager. v1 ships the `exec` provider (ops glue their cloud with a script or
  any CLI — Terraform, `aws`, whatever) and `noop` (emits events only, for
  dry-runs and "tell my external system" setups). A polished first-party
  `aws-asg` exec plugin follows as its own shippable unit.
- **Honest limitation**: node pools are still scheduling-inert, so "scale up
  group X" guarantees the *capacity* arrives, not that competing work can't
  land on it first. Fine for single-group clusters (the day-one case); the
  worker-groups/taints TODO is the real fix and this plan does not smuggle it
  in. Multi-group shortfall matching uses constraint compatibility and is
  documented as best-effort until pools are schedulable.

### 6.8 UI

**First-party pages, shipped with their phases:**

- Job page: request-vs-usage bars per task (phase 2); a Scaling panel —
  effective/spec/min/max count, which check decided, last 10 scaling events,
  "held by stabilization" state (phase 3).
- Nodes page: real utilization columns (phase 2); an Autoscaler page listing
  node groups, their desired/actual, recent provider actions (phase 6).
- Nav entries use the existing capability-probe pattern (the Observability
  section's approach in `ui/src/App.tsx:105-163`).

**Pushback on the generic mechanism.** "A job registers a sidebar link and
renders a panel in the main UI" is two very different features:

- The *link* is cheap and safe. v1 ships it: a jobspec block
  (`ui: { link: { title: 'Autoscaler', service: 'admin' } }`) surfaces an
  "Apps" section in the sidebar linking out to the job's own served UI
  (resolved via the service catalog). No third-party code in Legion's origin,
  no token exposure, a page refresh's worth of work. Caveat documented: the
  browser must be able to reach the service address (LAN/dev today; the
  ingress makes it universal later).
- The *panel* — third-party JS rendered inside the operator UI, with access to
  manager APIs — is a security boundary (the operator's bearer token is in
  `localStorage`; embedded code must be origin-isolated in a sandboxed iframe
  with a scoped, short-lived token and a postMessage contract, and the SPA
  isn't even served by the manager today). Designing that properly is a
  `ui-extensions-plan.md` of its own, listed as a non-goal here so it doesn't
  ship half-safe inside an autoscaler plan. The autoscaler needs nothing from
  it — being first-party, its UI is native.

### 6.9 Extensibility roadmap: from built-ins to plugins to the registry

v1 is deliberately built-ins-only behind trait seams (§6.5). The maturation
path, in order of trust required:

1. **Now**: sources `legion`/`prometheus`, strategies
   `target-value`/`per-replica`, providers `exec`/`noop`. Policy reuse via
   jsonnet imports.
2. **Dylib plugin kinds** (`plugins/metrics/`, `plugins/strategies/`) for
   operator-installed extensions, following the rollout-strategy checklist —
   but unlike rollout plugins, these traits take a config `Value` and expose
   `schema()`, fixing the config-blob gap that kind has. Manager-side, fully
   trusted, same story as every dylib today.
3. **Registry-distributed strategies are WASM, not dylib.** User-submitted
   policy *logic* executing in the manager is exactly the "full cluster
   compromise" the auth plan refused dylibs for. The mesh plan already
   designed the shape (sandboxed WASM filters, tiny host ABI, modules fetched
   as artifacts — `service-discovery-mesh-and-config.md` §D3); strategies are
   an even easier fit (pure `samples + count -> count` functions, no I/O in
   the ABI at all). When the registry plan lands, a shared "policy" is either
   a jsonnet snippet (config) or a WASM strategy (logic), both fetchable as
   artifacts. This plan only commits to keeping the strategy trait
   WASM-compilable (no host callbacks in the interface).

### 6.10 ACLs and security summary

- New resource `scale` (read/write, job-name scoped) — resize without deploy
  rights.
- `ForgetNode` under `node:write`; node-group status under `node:read`;
  provider configs live in manager config (operator-owned), never in job
  specs.
- Join tokens minted for scale-up are TTL'd and single-purpose; provider
  processes get the token on stdin, never logged (the existing join-token
  hygiene rules apply).
- The split-out agent's recommended policy is least-privilege:
  `scale:write` + `job:read` + `node:read` (+ `node:write` and `state:read`
  only if it runs node groups / uses SQL reads). Explicitly not `job:write`.
- Every new RPC is classified in `tests/integration/acl_endpoints.rs`; the build enforces
  it.

## 7. Proto, struct, config, and persistence changes

**Proto (`proto/legion.proto`)** — all with full public comments:

- `rpc ScaleTask(ScaleTaskRequest) returns (ScaleTaskReply)` — mutating,
  idempotency key, `scale:write`.
- `rpc GetJobScaleStatus(GetJobScaleStatusRequest) returns (GetJobScaleStatusReply)`
  — `scale:read`; per-task spec/override/effective counts, policy summary,
  evaluator state, recent scale events.
- `rpc ForgetNode(ForgetNodeRequest) returns (ForgetNodeReply)` — mutating,
  `node:write` (phase 6).
- `rpc GetNodeGroups(...)` — `node:read`; declared groups, desired/actual,
  last provider action (phase 6).
- `AllocationHealth` += `optional double cpu_used_mhz`,
  `optional double memory_used_mib`, `optional int64 sampled_at` (phase 2).
- `LedgerAllocation` / `GetJobInfo` reply += usage fields (phase 2).

**Structs / store**

- `TaskConfig` += `scaling: Option<ScalingConfig>` (`min`, `max`, `cooldown`,
  `scale_down`, `headroom`, `target`, `checks`, `activation` — serde camelCase,
  schemars for the jobspec schema).
- New table `task_scale` (§6.1), Scheduler-lane ownership; `scaling` config
  persists inside the existing `task_config` row (it is spec).
- New eval kind `job_scale` in the dispatch table (`src/structs/eval.rs`).
- `validate_job_config` gains scaling validation (min/max sanity, min-zero
  activation rule, keyed/`count.per` rejection). Cluster-aware checks
  (prometheus reachability) stay in manager admission, per the volume
  precedent.

**Config**

- `configs/jobs/maximal.jsonnet`: a `scaling` block (simple form on one task,
  full `checks` form on another) and a `ui.link` example.
- `configs/manager.jsonnet` (+ struct): `autoscaler { enabled,
  evaluationInterval, nodeGroups[] }`.
- Worker: `usageSampleSeconds` (default: heartbeat cadence).

**CLI / MCP / capabilities**

- `legion job scale`, `legion job scale-status`, `legion autoscaler status`,
  `legion autoscaler agent`, `legion node forget` — all with `--json`, routed
  through `output::resolve`, catalogued in `src/cmd/capabilities.rs`
  (`mutates` set correctly), MCP tools (`legion_job_scale` gated behind
  `--allow-write`).

**Events**: `scale.applied|deferred|clamped|check_failed`,
`nodegroup.scale_up|scale_in|provider_failed` added to the catalog
(`src/storage/events.rs`).

## 8. Implementation phases

Each ships alone, in order; later phases depend on earlier ones but not vice
versa.

1. **`legion job scale` + count overrides.** Table, effective-count accessor
   wired into all four count readers, `job_scale` eval (out and in), ownership
   rules (§6.2) including the rollout deferral, `scale` ACL resource, RPC +
   CLI + MCP, events, docs (`docs/autoscaling.md` first half). *Valuable
   alone: manual scaling stops being a deploy.*
2. **Usage collection.** Worker sampling, heartbeat/`allocation_health`
   fields, ledger/job-info read surface, CLI + UI request-vs-usage. *Valuable
   alone: the observability gap closes.*
3. **Task autoscaler MVP.** `scaling` block (simple form only: `target.cpu` /
   `target.memory`), evaluator loop in the manager (leader-gated, default-on),
   damping, status RPC, job-page Scaling panel, events. Legion source only.
4. **Checks, external metrics, scale-to-zero.** Full `checks` form,
   `prometheus` source, `per-replica` strategy, `activation` + `min: 0` +
   its validation, `headroom`, merge rule. S3 works end to end.
5. **Escape-to-job.** `legion autoscaler agent`, KV-lock leadership,
   `LEGION_MANAGER_ADDR` injection, the `autoscaler` policy recipe +
   control-plane binding docs, the escape-to-job pattern written up in
   `docs/` as the ecosystem-app template.
6. **Node autoscaling.** Placement-shortfall recording in the scheduler, node
   groups in manager config, provider exec ABI + `exec`/`noop` providers,
   scale-up with minted join tokens, drain-based scale-in, `ForgetNode`,
   `legion autoscaler status` node view + UI page. (First-party `aws-asg`
   provider ships separately after.)
7. **Extensibility + Apps links.** Dylib `metrics`/`strategies` plugin kinds
   (with `schema()`/config, closing the rollout-kind gap), `ui.link` Apps
   section. WASM strategies and registry distribution wait for the registry
   plan.

## 9. Testing plan

- **Unit (phase 1)**: override precedence over spec count; `base_count`
  ownership matrix (resubmit same/different/absent count × override
  present/absent × policy present/absent); clamp errors; slot scale-in picks
  highest slots and honors `retainOnScaleIn`; keyed/`count.per` rejection;
  `job_scale` eval add/retire math.
- **Unit (phases 3–4)**: target-value and per-replica math incl. tolerance
  band; max-of-desired merge; missing-data-never-scales-in; stabilization
  window (incl. cold-start conservatism); min-zero activation validation;
  cooldown edges.
- **Integration**: scale-out mid-rollout grows step targets next tick;
  scale-in mid-rollout defers then applies; autoscaler + manual scale interact
  through one path (last-writer, both audited); usage fields flow
  worker→heartbeat→ledger; leader failover pauses scale-in for one window;
  split-out agent drives the same scenarios over gRPC with a scoped token
  (and is *rejected* with `job:write`-less token on RunJob, proving the
  `scale` resource boundary); node group scale-up/in against a scripted fake
  `exec` provider incl. drain-then-terminate ordering and `ForgetNode`;
  provider timeout/failure emits events and never strands a draining node
  (uncordon on abort).
- **Regression gates**: every new RPC in `tests/integration/acl_endpoints.rs`; capability
  catalog test (`mutates` flags); scheduler benchmarks before/after the
  effective-count accessor (it sits on the hot path — AGENTS.md scheduler
  rule).

## 10. Rollout and backward compatibility

- No migrations of existing data; `task_scale` is a new table, absent rows
  mean "spec count", so every existing job behaves identically.
- The `scaling` jobspec field is additive; old specs validate unchanged. An
  old CLI talking to a new manager is unaffected; a new CLI's `job scale`
  against an old manager gets the standard unimplemented-RPC envelope error.
- `autoscaler.enabled` defaults `true` but the loop is inert without policies
  or node groups; operators who split the agent out set it `false` (the loop
  also logs a warning if it sees another actor's scale writes, catching the
  both-running misconfig).
- Worker/manager version skew: usage fields are optional proto fields; old
  workers simply report no usage, and policies over usage for those tasks show
  `failing` status rather than acting (missing data never scales in). Fits the
  existing handshake/skew policy in `docs/upgrades.md`.
- Docs: `docs/autoscaling.md` (surface, defaults, failure modes, limitations
  incl. the pool-inertness caveat and the min-zero wake rule),
  `docs/metrics.md` + `docs/observability.md` updates for usage,
  `docs/agent-readiness.md` for the new MCP tools, proto comments throughout,
  `maximal.jsonnet`.

## 11. Open questions

- **Should `job scale` support `--to-spec` semantics richer than `--reset`
  (e.g. pinning: "hold 6 and pause the policy")?** Lean: yes eventually —
  KEDA's pause annotation is well-loved — as `legion job scale web --pause`
  in phase 4, stored on the override row. Not in phase 1.
- **Per-job aggregate usage in `/metrics`?** Lean: yes, later, behind a config
  flag, aggregated per job (bounded cardinality), never per instance.
- **Does the `legion` source eventually read `QueryStateStore`-style
  expressions instead of a fixed vocabulary?** Lean: no — a fixed, documented
  vocabulary is supportable; arbitrary SQL in policies is a footgun and a
  privilege problem. The escape hatch is Prometheus.
- **Node-group scale-in using *actual* usage rather than requests?** Lean:
  requests-only in v1 (it's what the ledger knows and what CA does); revisit
  when node-level usage lands on `node_health`.
- **Autoscaling keyed instances** (policy drives `CreateInstances`/
  `DestroyInstance`)? Lean: the checks/strategy machinery reuses cleanly, the
  target differs; wait for a concrete user before designing the identity
  semantics of "which key gets destroyed on scale-in."
- **Where do cluster-level (node-group) policies live long-term?** Manager
  config is right for v1 (operator-owned, versioned with the cluster). If the
  UI ever needs to edit them, they become stored objects with CRUD RPCs —
  deliberately not designed now.

## 12. Acceptance criteria

- **P1**: `legion job scale web --count 6` changes running replicas with no
  new job config version and no rollout; resubmits follow the §6.2 matrix
  (each cell tested); scale-in mid-rollout defers with the documented message;
  a token with only `scale:write` can scale and cannot `job run`.
- **P2**: `legion job info` and the UI show live request-vs-usage for exec and
  container tasks on Linux and macOS; numbers survive manager failover.
- **P3**: the S2 jobspec holds a CPU-loaded task near target through load
  ramp-up and ramp-down, scale-in visibly damped; `legion job scale-status`
  explains every held/acted decision; disabling `autoscaler.enabled` stops
  evaluation.
- **P4**: the S3 jobspec sleeps at zero on an empty queue and wakes on lag via
  Prometheus; `min: 0` without an activating check is rejected at validation
  with the documented error.
- **P5**: the agent job (S6.6 spec verbatim) takes over from the in-core loop
  with the documented policy; killing the lock-holder replica fails over
  within the lock TTL.
- **P6**: with the fake exec provider, sustained unplaceable work provisions
  the computed node count from zero and the nodes join and fire `node_join`;
  idle nodes drain, terminate, and disappear from `legion node list`; aborting
  a scale-in uncordons.
- **P7**: a third-party dylib strategy loads by the standard checklist and its
  `schema()` errors surface at submit; a `ui.link` job shows in the Apps
  sidebar.
