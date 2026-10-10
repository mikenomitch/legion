# Stateful Workloads & Instance Identity

Legion schedules work as **allocations**. An allocation is a short-lived
placement of a task on a node. When a task is rescheduled (node loss, drain,
config update), it gets a new allocation with a new id. Stateful workloads
need a handle that survives that churn: replica N must always come back as
replica N, with replica N's data.

The `stateful` task block adds that handle, called the **instance**.

> Related: [volumes.md](volumes.md) (the storage an instance's handle points
> at), [workload-identity.md](workload-identity.md) (the instance id feeds the
> reschedule-stable token subject).
> Guides: [Postgres](guides/postgres.md), [Redis](guides/redis.md).

## Concepts

### Instance vs allocation

| | Allocation | Instance |
| --- | --- | --- |
| Lifetime | One placement, replaced on every reschedule | The workload's whole logical life |
| Id | Churning integer (`LEGION_ALLOC_ID`) | Stable string (`LEGION_INSTANCE_ID`) |
| Binds | A node + resources | Persistent state (`storage_handle`) + identity |

An instance has **at most one live allocation** at a time, and many over its
life. Allocations carry an `instance_id` reference. The instance row lives in
the manager's state store (`instance` table) and outlives any allocation.

### Slot mode vs keyed mode

- **`slot`** (StatefulSet or Nomad `per_alloc` style). A fixed pool of
  ordinal slots `0..count.num-1`. Each slot has a predictable, readable
  instance id:

  ```
  {job_id}-{group}-{slot}      e.g. stateful-demo-default-2
  ```

  Slot ids are **reused within the pool**. A replacement allocation for slot 2
  gets slot 2's instance id and storage handle. This is what you want for
  replica sets, since replica 2's disk belongs to replica 2.

- **`keyed`** (sandbox or dispatch style). You create instances on demand
  through the management API (`legion instance create` or
  `CreateInstance`). Each one gets a unique id that is **never recycled for
  the lifetime of the job**. A new sandbox can't inherit a dead one's identity
  or saved state while the job exists. (Destroying the job clears its
  instance history. See the [Id rules](#id-rules).) Keyed tasks define no
  `count`, and defining one is a parse-time error. The set of live instances
  sets the cardinality (zero instances means zero allocations). See
  [Keyed Mode: the Management API](#keyed-mode-the-management-api).

Legion guarantees at most one live allocation per instance. Every placement
binds inside the transaction that inserts the allocation. The one rollout
strategy that would run two live allocations at once, `createFirst`
(replace-before-stop), is rejected at job admission for stateful tasks. See
[Rollouts and the single-live-allocation
invariant](#rollouts-and-the-single-live-allocation-invariant).

### Instance lifecycle

```
pending -> active -> rescheduling -> active -> ... -> stopped/terminated
                          ^                              │
                          └────── start (keyed) ─────────┘  (stopped only)
```

- `pending`: the row exists but has never been placed.
- `active`: a live allocation fulfills it (`current_alloc_id` points at it).
  If the allocation sits on a **lost** node, the instance stays `active` and
  bound while the manager waits out the loss-confirmation window. During
  that wait, `legion instance get/list` shows a `pending-loss-confirmation`
  detail that explains it. See [Node loss and fencing
  (at-most-one)](#node-loss-and-fencing-at-most-one).
- `rescheduling`: its allocation died. The replacement will reuse the same
  instance id and storage handle.
- `stopped`: paused (keyed only). The allocation was stopped, but the row and
  storage handle are kept, so `legion instance start <id>` brings back the
  same identity with its data. A job stop puts keyed instances here. Stopped
  instances aren't scheduled until you start them.
- `terminated`: no longer desired (slot scale-in, keyed destroy, or job stop
  for slot rows). **Rows are kept.** Slot rows are kept so a later scale-out
  brings back the same id. Keyed tombstones are kept so a destroyed id is
  never recycled for the lifetime of the job.

## Job Spec Reference

```nickel
{
  name = "db",
  tasks = [{
    name = "replica",
    runtime = "exec",
    count = 3,                    # slot cardinality: slots 0, 1, 2
    stateful = {
      mode = "slot",              # 'slot' (default) | 'keyed'
      # idSource = "caller",     # keyed only: "caller" | "uuid"
      # initialInstances = ["user-1", "user-2"],  # keyed only: seed list
      onLost = {                  # optional; node-loss fencing policy — see
                                 # "Node loss and fencing (at-most-one)"
        selfTerminateAfter = "90s", # worker kills this task after losing
                                   # manager contact this long (default 90s)
        replaceAfter = "3m",        # manager replaces a lost-node allocation
                                   # only after this long; default 2x
                                   # selfTerminateAfter; must be greater than
                                   # selfTerminateAfter; "never" = wait for
                                   # node recovery or operator override
      },
      storage = {                 # optional persistent storage (docs/volumes.md)
        path = "/var/lib/db",     # container: bind destination; exec:
                                 # informational (read $LEGION_STORAGE_DIR)
        driver = "local-dir",     # volume driver; default 'local-dir'
        params = { size = "10G" }, # passed to the plugin verbatim
        retainOnScaleIn = true,   # default true (K8s whenScaled: Retain)
        retainOnStop = true,      # default true (K8s whenDeleted: Retain);
                                 # slot mode only — see retention section
      },
    },
    config = { command = ["run-db"] },
  }],
}
```

Validation rules (enforced at `job validate` and submit time):

- `mode` defaults to `'slot'`. Set `'keyed'` explicitly to use the
  instance-management model.
- `idSource` is only valid in keyed mode (`'caller'` or `'uuid'`).
- Slot mode requires `count.num >= 1` and can't be combined with `count.per`
  (per-attribute pools would collide on slot ids).
- Keyed mode **can't define `count` at all**. The spec is rejected at parse
  time with `task <name> is stateful keyed mode and cannot define count: keyed
  instance cardinality is driven by the instance management API (legion
  instance create/destroy), not the job spec`. There is no fixed pool to size,
  because the set of live instances is the cardinality.
- `initialInstances` is valid only in keyed mode. Each id must match
  `[A-Za-z0-9._-]{1,128}`, and the list can't contain duplicates.
- `storage.path` must be a non-empty absolute path.
- `storage.retainOnStop` is only valid in slot mode. Keyed stop and destroy
  behavior is fixed per instance: instance stop and job stop always keep the
  handle, and instance destroy always drops it. Setting the field on a keyed
  task is a validation error.
- At most one slot-mode stateful task per group (slot ids are derived from
  `(job, group, slot)`). Multiple keyed tasks per group are fine, because
  their ids are explicit keys rather than ordinals.
- `onLost` durations must parse (humantime strings like `'90s'`, `'3m'`).
  `selfTerminateAfter` must be greater than zero. `replaceAfter` must be
  **greater than** `selfTerminateAfter` (or the special value `'never'`).
  Otherwise a replacement could start before the worker is known to have
  stopped its copy. `onLost` belongs inside the `stateful` block. Declaring
  it at the task level, including on a stateless task, is rejected at parse
  time with a pointer to the right nesting.
- **The lost window must stay below the self-fence.** A node's lost window is
  `missedBeatsBeforeLost x its heartbeat interval`
  ([heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md)). If that
  reaches `selfTerminateAfter`, a healthy worker would kill its own stateful
  tasks between heartbeats. The manager refuses to start with a worker config
  that would cause this, and the error names both numbers. At the defaults
  (`5 x 2s = 10s` against a 90s fence) there is nine times the margin. A
  60-second batch cadence would need `selfTerminateAfter` raised well past its
  300-second window.

  Adaptive cadence (on by default) is not covered by that startup check. At
  the default `targetRatePerSecond`, a fleet past about 16,250 nodes is
  served 21s, so its window (105s) passes the 90s fence. A healthy worker
  still beats every 21s and only fences after about 70 seconds of failed
  retries, but the margin shrinks with every rung, and past about 68,750
  nodes the served interval itself nears the fence. See the known
  limitations in
  [heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md#known-limitations).

### Seed list (`initialInstances`) semantics

`stateful.initialInstances` (keyed mode only) is a **seed list**. It is not a
reconciled desired set. On every submit, after the config is saved and before
placement is evaluated, the manager makes sure each listed id exists:

- Ids that have **never existed** are created (`pending`) and go through the
  submit's normal placement eval.
- Ids that already exist in **any** state (live, `stopped`, or a
  `terminated` tombstone, even under another job) are skipped silently. This
  makes resubmits idempotent, and a **destroyed id is never brought back** by
  appearing in the seed list.

**Removing** an id from the list on a resubmit doesn't stop or destroy that
instance. Only `legion instance stop`/`destroy` (or a job stop/destroy)
changes a live instance's lifecycle. Read the list as "create these on first
run if they don't already exist."

## Environment Variables

The manager injects these into the task's runtime config `env` map at
dispatch time, so they are present at process start for `exec` and
`container` tasks:

| Variable | When | Meaning |
| --- | --- | --- |
| `LEGION_ALLOC_ID` | **all** tasks (stateless too) | Current allocation id. Changes with each placement. |
| `LEGION_INSTANCE_ID` | stateful tasks | Stable instance id. Survives reschedules. |
| `LEGION_INSTANCE_MODE` | stateful tasks | `slot` or `keyed`. |
| `LEGION_INSTANCE_SLOT` | slot mode only | Ordinal slot index `0..count-1`. |
| `LEGION_STORAGE_DIR` | tasks with `stateful.storage` | The instance volume's path. See [volumes.md](volumes.md#how-path-works-per-task-runtime) for why exec and container tasks get different values. |
| `LEGION_VOLUME_<NAME>` | per declared volume | Same, for every volume including `volumes[]` claims. |
| `LEGION_CLAIM_ID` | provisional starts only | The claim a workload was started under, before its placement was durable ([edge-boot.md](edge-boot.md)). Such a task has **no** `LEGION_ALLOC_ID` yet, and Legion doesn't set a placeholder. `LEGION_INSTANCE_ID` is correct from the start. |

Key your app's durable state and "who am I" decisions on
`LEGION_INSTANCE_ID`. `LEGION_ALLOC_ID` is the short-lived placement id.

**Precedence:** Legion-injected variables override user-declared env keys of
the same name. If a jobspec could shadow them, a workload could misreport its
own identity. Nomad works the same way: its runtime variables always take
precedence.

## Reschedule Semantics

When a slot's allocation dies (process failure, node loss, drain):

1. The instance moves from `active` to `rescheduling`. Its `instance_id` and
   `storage_handle` don't change.
2. The scheduler plans a replacement allocation as usual (constraints,
   resources, scoring). Slot identity doesn't influence node choice in v1.
3. The replacement is bound to the lowest free slot, which is the one that
   just lost its allocation. It reuses the same instance id and storage
   handle, and the instance moves back to `active`.

Config updates work the same way. Instances are keyed by job, not by job
config version, so replica N keeps its identity and storage across deploys.

**Node loss is the exception.** A live worker reports a dead process, and
Legion replaces it right away. A missing heartbeat only proves the manager
can't reach the node, and the workload may still be running. So stateful
allocations on lost nodes wait out a loss-confirmation window before step 1
happens. See the next section.

### Preemption

A stateful allocation on **cluster-reachable** storage (`nfs`, and future
network drivers) can be preempted. Preemption stops it the same way a rollout
replacement does: a clean, manager-driven stop using the task's `stop`
ladder. Its replacement reattaches the same storage handle wherever it lands,
through the reschedule path above.

A stateful allocation on **node-local** storage (`local-dir`) is **never**
preempted. Evicting one either frees nothing (the instance can only be
recreated on the node that holds its data) or abandons the data. This follows
the same rule as drain's handling of stateful work: Legion never force-moves
protected state. Plan around one consequence: a node filled with low-band,
node-pinned stateful work can't be preempted, and the placement error says
so. See [preemption-and-eviction.md](preemption-and-eviction.md).

## Node loss and fencing (at-most-one)

Single-writer systems need to know that slot N's old allocation is dead
before a replacement takes slot N's identity, storage handle, and
workload-identity `sub`. A missed heartbeat can't tell you that. A network
partition looks the same as a dead node, and replacing a partitioned node's
allocation right away would run the same instance twice (split-brain).
Legion handles this with **lease-based fencing**: two timers that work
together, one on each side of the partition.

1. **Worker self-fence** (`onLost.selfTerminateAfter`, default `90s`). If a
   worker can't complete a heartbeat to the manager for longer than this, it
   kills its stateful allocations locally. The heartbeat loop never exits on
   failure. It retries with backoff forever, while a separate watchdog tracks
   the time since the last successful heartbeat. The fenced allocation is
   recorded as failed, so the first heartbeat after reconnecting reports it
   as failed or stopped rather than running.

   The window survives worker restarts. Tasks keep running when the worker
   process exits, so the worker persists the wall-clock time of its last
   successful heartbeat (`last_manager_contact`, beside
   `worker_state.json`). A restarted worker that can't reach the manager
   yet, whether it is waiting in its connect loop or crash-looping, fences
   restored stateful tasks against that time instead of starting a fresh
   window. Once the manager answers and the worker re-registers, the normal
   watchdog takes over. Registration resets the node's loss on the manager
   too, so both clocks restart together.

   Because this record is a wall clock, a large clock step on the worker
   host while the worker is down moves the restart-time fence. A step
   forward fences early, which is safe. A step backward delays the fence by
   the size of the step. The in-process watchdog uses a monotonic clock and
   is unaffected.
2. **Manager confirmation gate** (`onLost.replaceAfter`, default
   `2 x selfTerminateAfter`). A stateful allocation on a lost node is **not**
   freed for replacement until the node has been lost longer than this. The
   node row records `lost_at` when the node is marked lost. It's cleared when
   the node heartbeats again or re-registers. The monitor loop that marks
   nodes lost also re-checks gated allocations every tick, so replacement
   happens automatically once the window passes, with no operator action.

**Why this confirms the loss:** validation requires
`replaceAfter > selfTerminateAfter`. By the time the manager frees the
allocation, one of two things is true on the unreachable node. Either the
worker is alive but partitioned and has already self-fenced (its window
expired earlier, by the manager's own clock plus the safety margin), or the
worker is dead and so is the workload. In both cases the replacement can't
run at the same time as the original. You get at most one live allocation
per instance, even across partitions.

While an allocation is gated, nothing visible changes. The allocation row
stays, the instance stays `active` and bound, and `legion instance get/list`
shows a `pending-loss-confirmation` detail naming the lost node and what
would unblock it. Stateless tasks aren't affected by any of this. They have
no fencing window and are replaced as soon as their node is marked lost.

Watch out for one case. A **config update** replaces every allocation,
including ones on an unreachable node that can't be told to stop. If you
deploy while a node is partitioned, the old allocation there may overlap its
replacement for up to `selfTerminateAfter`. The worker's self-fence still
caps the overlap, which is why the window exists. Avoid deploying stateful
jobs while nodes are lost, or set `selfTerminateAfter` to an overlap your
workload can tolerate.

### Defaults, tuning, and `'never'`

Every stateful task gets this policy. Without an `onLost` block, the values
are `selfTerminateAfter = "90s"` and `replaceAfter = "180s"`. **Because the
defaults favor safety, stateful failover is slower than stateless
failover.** A replica on a lost node waits out the confirmation window (about
3 minutes by default), while a stateless task moves in seconds. If fast
failover matters more than a long partition grace period (and your checks
and restarts can handle it), lower the worker side. For example,
`selfTerminateAfter = "15s"` makes `replaceAfter` default to 30s. The cost is
less tolerance for manager downtime: any manager outage longer than the
window kills the task locally. It's rescheduled when the manager returns.

`replaceAfter = "never"` matches K8s StatefulSet behavior: the manager never
replaces the allocation on its own. Recovery requires the node to come back
(the allocation keeps running and `lost_at` clears) or an operator override.

### Operator override

`legion instance replace <id>` (RPC `ReplaceInstance`, MCP
`legion_instance_replace`, supports `--idempotency-key`) is like
force-deleting a pod stuck on a lost K8s node. It frees the gated allocation
immediately, skipping the rest of the window and overriding `'never'`. It
then enqueues a reschedule that excludes the lost node, so the replacement
re-binds the same instance id and storage handle somewhere else. It refuses
to act unless the instance's allocation is on a `lost` node. Use it only when
you've confirmed the node is dead or isolated. Forcing it while the node is
only partitioned, and the worker hasn't self-fenced yet, causes the
split-brain the gate is there to prevent.

### State walkthrough

`db-default-2` is `active` on `node-w3` with the default policy
(self-fence 90s, replace 180s) when `node-w3` drops off the network:

| t | manager view | worker view (partitioned) |
| --- | --- | --- |
| 0s | heartbeats from `node-w3` stop arriving | heartbeats to the manager start failing and retry with backoff |
| ~10s | monitor marks `node-w3` lost and sets `lost_at`. Stateless allocations are replaced now. `db-default-2` is gated (`pending-loss-confirmation` in `instance get`) | task still running, self-fence clock at ~10s |
| 90s | still gated (90 < 180) | watchdog kills the allocation: `self-fencing allocation …: lost manager contact for 90s` |
| 180s+ | monitor tick confirms the loss: allocation freed, instance `rescheduling`, replacement planned on a live node, instance `active` again with the **same id and storage handle** | nothing running |
| later | if `node-w3` reconnects, its heartbeat reports the fenced allocation as failed and clears `lost_at`. The manager treats it as a normal dead allocation (it may already have been replaced) | worker rejoins cleanly |

If the node had returned before 180s, the gate would release. The returning
worker reports the allocation (already self-fenced at 90s, or still running
if the outage was shorter than 90s), and normal reconciliation takes over. An
allocation that never stopped keeps running with its binding intact, and no
replacement is planned.

## Rollouts and the single-live-allocation invariant

The rule of at most one live allocation per instance must hold during
rollouts too. The only rollout strategy that would break it is `createFirst`
(replace-before-stop). It starts the new allocation before stopping the old
one. For a stateful task, that means two live allocations sharing one
instance id, storage handle, and workload-identity subject at the same time.
The node-loss fencing in [Node loss and fencing
(at-most-one)](#node-loss-and-fencing-at-most-one) doesn't help here,
because a `createFirst` overlap is intentional and happens on healthy nodes.

So **a stateful task can't use a `createFirst` rollout.** Legion rejects the
combination at job admission with an error that points to this rule.
Stateful groups must use the default stop-then-start (`destroyFirst`)
replacement, which guarantees a single live allocation.

### Edge boot does not weaken the invariant

[Edge boot](edge-boot.md) lets a node start a keyed sandbox before its
placement is durable. That might seem to threaten the same rule, but it
doesn't. A start that hasn't been ratified binds nothing: no instance id, no
storage handle, no workload identity. Binding still happens in one place,
inside the plan apply. A ratification is an ordinary plan submission that
names the instance it's competing for.

If two nodes both boot `user-42`, they resolve the same way two concurrent
plans do. The store serializes them, the first one binds, and the second is
refused with `instance_claimed` and kills its work.

## Ordered rollouts for slot pools

Slot pools are ordinal, and quorum systems care about the order in which
replicas restart. Two rollout options make config updates follow slot order.
Set them under `rollout.slots`, or per group under
`rolloutByGroup.<group>.slots` like every other rollout option:

```nickel
rollout = {
  slots = {
    order = "sequential",   # 'parallel' (default, today's behavior) | 'sequential'
    updateFrom = 2,         # optional staged floor; requires order = "sequential"
  },
}
```

### `order = "sequential"` — one slot at a time, highest first

A sequential rollout replaces allocations **one slot per step, highest
ordinal first** (`count-1 → 0`). Each step waits for the previous slot's
replacement to pass the rollout's existing health criteria (allocation
health snapshots plus `minHealthyTime`, like any other rollout step). This
matches the update order of a K8s StatefulSet's `OrderedReady` pod
management: followers (high ordinals) restart before the usual leader at
slot 0.

- **Each slot is stopped, then started.** The slot's old allocation is
  retired, and its replacement is placed, **re-binds the same instance id and
  storage handle**, and must become healthy before the next (lower) slot is
  touched. (`createFirst` is still rejected for stateful groups, so the
  one-live-allocation rule holds at every step.)
- **Halt on unhealthy.** If a replacement never passes the health gate, the
  rollout stops at that slot, and lower slots keep running the old config.
  Standard rollout failure behavior applies. Without a `healthyDeadline`, the
  rollout waits (status `running`, with a `waiting for slot N replacement to
  become healthy` event). With one, the configured `onHealthDeadline` action
  (`rollback` by default) fires.
- **Visibility.** Rollout events name each step's slot (`retired old
  allocation for slot 2`, `slot 2 updated and healthy`), and
  `GetRolloutStatus` reports `step_index`/`total_steps`, where total steps is
  the number of slots being updated.
- Sequential has its own one-slot-per-step progression, so you can't combine
  it with the step modes (`percentEachStep`, `countEachStep`,
  `percentSteps`, `countSteps`) in the same scope. A per-group `slots`
  override replaces a global step mode (and the reverse), the same way step
  mode overrides already work. `progression = "manual"` works with it: each
  manual advance releases one slot.

**Initial bring-up is not gated yet.** A new job's slots are placed in
ascending order (0 → N-1), but in a single eval without per-slot health
gating. There is no equivalent of K8s `OrderedReady` bring-up yet. Today
`order = "sequential"` applies to config updates (and staged promotions).

### `updateFrom` — staged ("partitioned") rollouts

`updateFrom = N` stages the rollout. **Only slots `>= N` are updated to the
new config. Slots `0` through `N-1` keep running their current allocations untouched**,
including across later reschedules. (The scheduler knows staged slots are
intentionally held by the older config and won't double-place them.) This is
the equivalent of K8s StatefulSet `rollingUpdate.partition`, and it supports
a canary-then-promote workflow:

```text
count: 4, v2 submitted with updateFrom: 2
  step 1: slot 3 → v2 (health-gated)
  step 2: slot 2 → v2 (health-gated)
  rollout completes; slots 1 and 0 still run v1   ← canary state

operator validates v2 on slots 3 and 2, then resubmits with updateFrom: 0
  (or omits it): a new rollout updates the remaining slots in order
  3 → 2 → 1 → 0 (slots already on the new task content are re-placed on the
  new config version), each gated on health. Lowering the floor is all it
  takes — the promote needs no other spec change.
```

Validation rules (all at `job validate` and submit time, with actionable
errors):

- `rollout.slots` (both `order` and `updateFrom`) is only valid for groups
  whose stateful task uses **slot mode**. Stateless and keyed groups are
  rejected, with a pointer to `rolloutByGroup` scoping for mixed jobs.
- `updateFrom` requires `order = "sequential"`. (K8s `partition` exists only
  inside ordered rolling updates. "Update slots >= N" doesn't make sense
  unless slots are replaced by ordinal.)
- `updateFrom` must be `< count.num`. K8s allows `partition >= replicas` to
  mean "update nothing." In Legion, a rollout that finishes instantly while
  pinning every slot to the old config is almost always a typo. Omit the
  field to update everything, or lower it to stage.
- `updateFrom = 0` is valid and the same as omitting the field.
- Negative or non-numeric values and unknown `order` values are rejected at
  parse time.

### K8s mapping

| K8s StatefulSet | Legion |
| --- | --- |
| `podManagementPolicy: OrderedReady` (update order) | `rollout.slots.order = "sequential"` |
| `updateStrategy.rollingUpdate.partition` | `rollout.slots.updateFrom` |
| `podManagementPolicy: Parallel` | `order = "parallel"` (the default) |
| OrderedReady bring-up gating | not yet (slots place 0→N-1 but ungated) |

## Keyed Mode: the Management API

Keyed mode is the building block for sandboxes and dispatch work. A job can
have anywhere from zero to thousands of long-lived logical instances per
(job, group, task), created and destroyed on demand. Each has a unique id
that is **never recycled for the lifetime of the job**.

### Lifecycle

```
CreateInstance ──> pending ──placed──> active ──alloc dies──> rescheduling
                     ^                   │ ^                        │
   StartInstance ────┘    StopInstance / │ └──replacement re-binds──┘
   (from stopped)         job stop       v
                                      stopped ──StopInstance/DestroyInstance──┐
                                         │                                     v
                          DestroyInstance / job destroy ──> terminated (tombstone,
                                                             for the job's lifetime)
```

- **Create** registers the instance (state `pending`) and enqueues placement.
  The scheduler sets a keyed task's desired allocation count from its live
  (pending, active, or rescheduling) instances. Keyed tasks define no
  `count`, and a keyed task with zero instances runs zero allocations.
  Stopped instances are **not** in the desired set.
- **Placement** binds the new allocation to an unbound live instance, inside
  the same transaction that inserts the allocation (the same path slot mode
  uses). If the task declares `stateful.storage`, the storage handle is
  created on first placement.
- **Failure** of the allocation moves the instance to `rescheduling`. The
  replacement allocation re-binds the same instance id and storage handle.
  Allocation churn never terminates or stops a keyed instance.
- **Stop** (`StopInstance` or `legion instance stop`) pauses the instance. It
  stops the allocation through the normal allocation-stop path and moves the
  row to `stopped`, **keeping the row and its storage handle**. The id stays
  owned (creates with that id still collide), and the instance isn't
  scheduled until it's started. See [Stopping and
  starting](#stopping-and-starting).
- **Start** (`StartInstance` or `legion instance start`) reverses a stop. The
  instance moves from `stopped` to `pending` and is placed again with the
  same id and kept handle.
- **Destroy** stops the instance's allocation through the normal
  allocation-stop path, **drops the storage handle**, and keeps the row as a
  `terminated` tombstone for the lifetime of the job. Destroying the job
  clears it.

### API / CLI

Eight manager RPCs back this: `CreateInstance`, `CreateInstances` (batch),
`StopInstance`, `StartInstance`, `DestroyInstance`, `ReplaceInstance`,
`ListInstances`, and `GetInstance`. See `proto/legion.proto` for the full
contract. They're available as `legion instance ...` commands and as MCP
tools (`legion_instance_create/stop/start/destroy/replace/list/get`):

```bash
# create with an explicit key (idSource 'caller' requires one; with 'uuid'
# or unset the server mints an id when --id is omitted)
legion instance create sandbox-demo --id user-42 -j
# => { "ok": true, "data": { "instanceId": "user-42", "deduplicated": false } }

# Batch create: repeated --id flags and/or --count switch to the atomic
# CreateInstances RPC (all-or-nothing). This creates user-a, user-b, plus 3
# server-minted ids — 5 instances, or none if any id collides.
legion instance create sandbox-demo --id a --id b --count 3 -j

legion instance list sandbox-demo -j
# => { "ok": true, "data": [ { "instanceId": "user-42", "jobId": "sandbox-demo",
#      "group": "sandboxes", "task": "sandbox", "mode": "keyed",
#      "state": "active", "currentAllocId": 7,
#      "storageHandle": "local-dir/…", "createdAt": "…", "updatedAt": "…" } ] }

legion instance get user-42 -j

# Pause (reversible): stop the allocation, retain row + storage handle
legion instance stop user-42 -j        # state -> "stopped"
# Resume: re-place against the same id and retained handle
legion instance start user-42 -j       # state -> "pending" -> "active"

legion instance destroy user-42 -j     # forever (tombstone, handle dropped)
# => { "ok": true, "data": { "message": "Destroyed instance user-42",
#      "deduplicated": false } }
```

`create`, `destroy`, `stop`, and `start` accept `--idempotency-key` and report
`deduplicated`, like the other mutating RPCs. Destroying an already-terminated
instance, stopping an already-stopped one, and starting an already-live one
all succeed and do nothing. You only need `--group`/`--task` when the job
declares more than one keyed task. If the target is ambiguous, the command
fails instead of guessing.

### Stopping and starting

`stop` and `start` work in keyed mode only. (In slot mode, the scheduler
manages the lifecycle through `count.num`.) They let you pause a keyed
instance and resume it later, unlike `destroy`, which is permanent:

| | `stop` | `destroy` |
| --- | --- | --- |
| Row | retained (`stopped`) | retained (`terminated` tombstone) |
| Storage handle | **retained** | **dropped** |
| Id | stays owned (creates collide) | stays owned for the job's lifetime |
| Reversible | yes, via `start` | no (create a new id instead) |
| In desired set | no (not scheduled until started) | no, ever |

- **Stop** removes the instance's allocation in the same transaction that
  sets the row to `stopped`, so the release hook can't race it back to
  `rescheduling`. While stopped, the instance doesn't count toward the keyed
  task's desired count. Resubmits, config updates, and reschedules won't
  bring it back. Only `start` does.
- **Start** moves the instance from `stopped` to `pending` and enqueues
  placement. The new allocation re-binds the same instance id and its kept
  storage handle, so the revived sandbox is the same logical instance with
  its data.
- **Terminated** (destroyed) instances can't be stopped or started. The id is
  tombstoned for the job's lifetime, so create a new id instead.

To stop an instance and bring it back:

```bash
legion instance create sandbox-demo --id user-42   # active
legion instance stop user-42                        # paused, handle kept
legion instance get user-42 -j                      # state: "stopped"
legion instance start user-42                        # re-placed, same handle
legion instance get user-42 -j                      # state: "active" again

# A job stop is a bulk pause for keyed instances:
legion job stop sandbox-demo                         # keyed -> stopped (handles kept)
legion job run configs/jobs/sandbox-demo.ncl     # job revived...
legion instance list sandbox-demo -j                 # ...but instances still "stopped"
legion instance start user-42                        # each one revived explicitly
```

Resubmitting a stopped job (`job run`) clears the job's stopped status, but it
**doesn't** start its stopped keyed instances. Each one needs an explicit
`instance start`, so you choose which sandboxes come back.

### Id rules

- Allowed characters are `[A-Za-z0-9._-]`, and length is 1–128 (ids end up in
  env vars, workload identity subjects, and CLI args).
- `stateful.idSource` sets the default source. `'caller'` makes the id
  required on create. `'uuid'` (or unset) lets the server create one. A valid
  caller-supplied id is always used regardless of `idSource`.
- Ids are unique across the whole cluster and **never recycled for the
  lifetime of the job that owns them**. Creating an id that matches any
  existing instance row, in this job or another one, is rejected
  (`AlreadyExists`). That includes a `stopped` instance or a `terminated`
  tombstone. So a dead sandbox's identity (and its WI `sub`) can never
  be given to a new one while the job exists. Destroying the whole job (`job
  destroy`) clears its instance history, since rows are deleted with the job
  (see [Scale-In / Scale-Out Retention](#scale-in--scale-out-retention)).
  After that, ids from that job are free again. The never-recycle guarantee
  lasts for the job's lifetime only.
- Keyed ids can't match the reserved slot-id pattern `{job}-{group}-{slot}`
  of a slot-mode stateful task **in the same job**. Slot and keyed ids are
  kept separate so a keyed id can never take over, or be taken over by, a
  slot placement. A create with such an id is rejected, and a slot binding
  never adopts a row it doesn't own.

### Storage on destroy

Destroy always drops the storage handle. Slot scale-in does the opposite and
keeps storage by default for a later scale-out. The difference is that a slot
id will run again and must reattach its data. A destroyed keyed id can never
run again, so keeping its state attachable would leak storage.

Stopping the whole job (`job stop`) is **not** a destroy. Keyed instances move
to `stopped` (not `terminated`) and **keep** their storage handles, so `job
run` plus `instance start` brings them back with their data. Slot instances
become `terminated` and also keep their handles (unless the task sets
`retainOnStop = false`, see [Scale-In / Scale-Out
Retention](#scale-in--scale-out-retention)), so a later `job run` brings back
the same ordinal identities. Only `instance destroy` or `job destroy` drops
keyed handles.

### Count interaction

Keyed tasks have no `count`. Defining one is a parse-time error (see the
validation rules above), because the instance management API owns
cardinality. Submitting the job places nothing until you create instances.
Config updates and rollouts bring each keyed task to its live-instance count
and replace every live instance's allocation with one on the new config.

## Scale-In / Scale-Out Retention

- **Scale-in** (`count` 3 → 2): slot 2's instance is marked `terminated`, but
  the **row is kept**, and its storage handle is kept by default
  (`retainOnScaleIn = true`). With `retainOnScaleIn = false`, the handle is
  dropped on scale-in. (Reclaiming the storage is up to the driver, see
  below.)
- **Scale-out** (2 → 3): slot 2 comes back with the **same instance id**, and
  a kept storage handle is reattached. If no handle was kept, a new one is
  created.
- **Job stop**: rows are kept for all instances. Slot instances become
  `terminated`, so a later `job run` brings back the same identities. Their
  handles are kept by default (`retainOnStop = true`), so the re-run
  reattaches the same data. With `retainOnStop = false`, handles are dropped
  on stop and the revived slots get new ones: the identity comes back, but
  the data doesn't. Keyed instances become `stopped` (handles always kept),
  so `job run` plus `legion instance start <id>` brings each one back with its
  data (see [Stopping and starting](#stopping-and-starting)). A stop is a
  pause.
- **Job destroy**: instance rows are deleted with the job (FK cascade),
  storage handles included, regardless of `retainOnStop`. That setting only
  affects `job stop`. Job destroy is the only thing that clears a job's
  keyed-id history, so the never-recycle guarantee holds for the lifetime of
  the job and no longer.

### K8s mapping

The two settings are the slot-mode equivalent of a StatefulSet's
`persistentVolumeClaimRetentionPolicy` (both default to Retain):

| K8s | Legion |
| --- | --- |
| `whenScaled: Retain/Delete` | `retainOnScaleIn = true/false` |
| `whenDeleted: Retain/Delete` | `retainOnStop = true/false` (applies to `job stop`, while `job destroy` always deletes) |

`whenDeleted` maps to `job stop` rather than `job destroy`. Stopping a job is
Legion's equivalent of deleting a StatefulSet: the spec stays, and identities
can come back. `job destroy` always purges the job's entire record history.

## Storage Handle Contract

Each instance can own a `storage_handle`, created on first placement when the
task declares `stateful.storage`. It is a `{plugin}/{volume_id}` pair
(`local-dir/9f2c…`) that names the driver that owns the storage and the
volume. The contract:

- the handle binds to the **instance**, never the allocation
- every allocation for the instance sees the same handle
- handles survive reschedules, config updates, and (by default) scale cycles

Behind the handle is a volume, set up by a plugin on whatever node the
instance lands on. It's mounted before the task starts, unmounted on stop,
and reclaimed when the handle is dropped. Node-local volumes also **pin**
their instance to the node holding the data, so a replacement can never start
on an empty disk. See **[docs/volumes.md](volumes.md)** for plugins, the
driver contract, and `legion volume`.

Every task runtime uses the same storage machinery. No runtime has its own
private storage. What differs is the shape a runtime expects
([`volume_mode`](volumes.md#filesystem-mode-vs-block-mode)). `exec` and
`container` use a host directory. The [`sandbox`
runtime](task-runtime-sandbox.md) declares `mode = "block"` and gets a
formatted disk image that its microVM attaches as a virtio-blk device. In
both cases, single-writer safety comes from the rule above. A filesystem (or
a filesystem image) supports only one writer, and at most one live
allocation per instance guarantees that.

## Workload Identity Integration

The instance id is the stable key that OIDC workload identity uses (see
[workload-identity.md](workload-identity.md)). Stateful claims carry
`legion.io/instance` (and `legion.io/slot` in slot mode), and `sub` templates
on stateful tasks can use the `{instance}` and `{slot}` placeholders. Since
the instance id survives reschedules, the token's identity stays stable even
though each allocation gets fresh short-lived tokens.

## Trying It Locally

Start a manager, join a worker, and run the demo job:

```bash
# Terminal 1: manager
cargo run -- start manager -c configs/manager.ncl

# Terminal 2: worker (use the join token printed by the manager)
cargo run -- join <TOKEN>

# Terminal 3: run the slot-mode demo (count 3, stateful storage)
cargo run -- job run configs/jobs/stateful-demo.ncl
cargo run -- job list -j
```

Each replica logs its identity at start, for example:

```
instance=stateful-demo-default-1 slot=1 alloc=12
```

Inside the task the env contains:

```
LEGION_INSTANCE_ID=stateful-demo-default-1
LEGION_INSTANCE_SLOT=1
LEGION_INSTANCE_MODE=slot
LEGION_ALLOC_ID=12
```

Kill the worker process hosting slot 1 (or stop its allocation). The
replacement comes up with the **same** `LEGION_INSTANCE_ID` and slot but a
new `LEGION_ALLOC_ID`. You can see instance rows in the state store:

```bash
cargo run -- state query 'SELECT instance_id, state, slot_index, current_alloc_id, storage_handle FROM instance' -j
```

### Keyed mode walkthrough

With the same manager and worker running:

```bash
# Submitting the keyed demo places ZERO allocations (count is not in charge)
cargo run -- job run configs/jobs/sandbox-demo.ncl
cargo run -- instance list sandbox-demo        # empty

# Create two sandboxes: one explicit key, one server-minted
cargo run -- instance create sandbox-demo --id user-42
cargo run -- instance create sandbox-demo
cargo run -- instance list sandbox-demo        # both active, distinct handles

# Reschedule stability: kill user-42's allocation (worker restart or
# `state query` to find the alloc) — the replacement re-binds user-42's id
# and storage handle.

# Destroy: allocation stops, handle dropped, id tombstoned for the job's life
cargo run -- instance destroy user-42
cargo run -- instance get user-42 -j           # state: "terminated", no handle
cargo run -- instance create sandbox-demo --id user-42
# => error: AlreadyExists — destroyed ids are not recycled while the job lives
#    (job destroy would clear the history and free the id)
```

## Stateless Jobs Are Unaffected

Tasks without a `stateful` block behave as before. No instance rows are
created, allocations carry no `instance_id`, and no `LEGION_INSTANCE_*`
variables are injected. The only change for stateless tasks is the
`LEGION_ALLOC_ID` variable.
