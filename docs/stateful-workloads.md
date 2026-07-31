# Stateful Workloads & Instance Identity

Legion's scheduling unit is the **allocation** — an ephemeral placement of a
task on a node. When a task is rescheduled (node loss, drain, config update)
it gets a *new* allocation with a *new* id. Stateful workloads need a handle
that survives that churn: replica N must always come back as replica N, with
replica N's data.

The `stateful` task block introduces that handle: the **instance**.

> Design plan: [stateful-workloads-and-instance-identity-plan.md](../internal_docs/plans/stateful-workloads-and-instance-identity-plan.md).
> Related: [volumes.md](volumes.md) (the storage an instance's handle points
> at), [workload-identity.md](workload-identity.md) (the instance id feeds the
> reschedule-stable token subject).
> Guides: [Postgres](guides/postgres.md), [Redis](guides/redis.md).

## Concepts

### Instance vs allocation

| | Allocation | Instance |
| --- | --- | --- |
| Lifetime | One placement; replaced on every reschedule | The workload's whole logical life |
| Id | Churning integer (`LEGION_ALLOC_ID`) | Stable string (`LEGION_INSTANCE_ID`) |
| Binds | A node + resources | Persistent state (`storage_handle`) + identity |

An instance has **at most one live allocation** at a time and many over its
life. Allocations carry an `instance_id` reference; the instance row lives in
the manager's state store (`instance` table) and outlives any allocation.

### Slot mode vs keyed mode

- **`slot`** (StatefulSet / Nomad `per_alloc` style) — implemented. A fixed
  pool of ordinal slots `0..count.num-1`. Each slot has a deterministic,
  human-legible instance id:

  ```
  {job_id}-{group}-{slot}      e.g. stateful-demo-default-2
  ```

  Slot ids are **reused within the pool**: a replacement allocation for slot 2
  gets slot 2's instance id and storage handle. That reuse is the *correct*
  behavior for replica sets — replica 2's disk is replica 2's disk.

- **`keyed`** (sandbox / dispatch style) — implemented. Instances are created
  on demand via the management API (`legion instance create` /
  `CreateInstance`), each with a unique id that is **never recycled for the
  lifetime of the job**, so a new sandbox can never inherit a dead one's
  identity or persisted state while the job lives (destroying the job clears
  its instance history; see the [Id rules](#id-rules)).
  keyed tasks define no `count` (a parse-time error): the set of live instances
  (zero instances = zero allocations). See
  [Keyed Mode: the Management API](#keyed-mode-the-management-api).

An instance has **at most one live allocation at a time, by construction**:
every placement binds inside the allocation-insert transaction, and the one
rollout strategy that would run two live allocations concurrently —
`createFirst` (replace-before-stop) — is rejected at job admission for stateful
tasks (see [Rollouts and the single-live-allocation
invariant](#rollouts-and-the-single-live-allocation-invariant)).

### Instance lifecycle

```
pending -> active -> rescheduling -> active -> ... -> stopped/terminated
                          ^                              │
                          └────── start (keyed) ─────────┘  (stopped only)
```

- `pending` — row exists, never placed.
- `active` — a live allocation fulfills it (`current_alloc_id` points at it).
  An instance whose allocation sits on a **lost** node stays `active` and
  bound while the manager waits out the loss-confirmation window — `legion
  instance get/list` then carries a `pending-loss-confirmation` detail
  explaining the wait (see [Node loss and fencing
  (at-most-one)](#node-loss-and-fencing-at-most-one)).
- `rescheduling` — its allocation died; the replacement will reuse the same
  instance id and storage handle.
- `stopped` — paused (keyed only): the allocation was stopped but the row AND
  storage handle are retained, so `legion instance start <id>` revives the
  same identity with its data. A job stop puts keyed instances here. Not part
  of the scheduler's desired set until explicitly started.
- `terminated` — no longer desired (slot scale-in, keyed destroy, or job
  stop for slot rows). **Rows are retained**: slot rows so a later scale-out
  re-materializes the same id, keyed tombstones so a destroyed id is never
  recycled for the lifetime of the job.

## Job Spec Reference

```jsonnet
{
  name: 'db',
  tasks: [{
    name: 'replica',
    runtime: 'exec',
    count: 3,                    // slot cardinality: slots 0, 1, 2
    stateful: {
      mode: 'slot',              // 'slot' (default) | 'keyed'
      // idSource: 'caller',     // keyed only: 'caller' | 'uuid'
      // initialInstances: ['user-1', 'user-2'],  // keyed only: seed list
      onLost: {                  // optional; node-loss fencing policy — see
                                 // "Node loss and fencing (at-most-one)"
        selfTerminateAfter: '90s', // worker kills this task after losing
                                   // manager contact this long (default 90s)
        replaceAfter: '3m',        // manager replaces a lost-node allocation
                                   // only after this long; default 2x
                                   // selfTerminateAfter; must be greater than
                                   // selfTerminateAfter; 'never' = wait for
                                   // node recovery or operator override
      },
      storage: {                 // optional persistent storage (docs/volumes.md)
        path: '/var/lib/db',     // container: bind destination; exec:
                                 // informational (read $LEGION_STORAGE_DIR)
        driver: 'local-dir',     // volume driver; default 'local-dir'
        params: { size: '10G' }, // passed to the plugin verbatim
        retainOnScaleIn: true,   // default true (K8s whenScaled: Retain)
        retainOnStop: true,      // default true (K8s whenDeleted: Retain);
                                 // slot mode only — see retention section
      },
    },
    config: { command: ['run-db'] },
  }],
}
```

Validation rules (enforced at `job validate` / submit time):

- `mode` defaults to `'slot'`; declare `'keyed'` explicitly to switch to the
  instance-management model.
- `idSource` is only valid in keyed mode (`'caller'` or `'uuid'`).
- Slot mode requires `count.num >= 1` and cannot be combined with `count.per`
  (per-attribute pools would collide on slot ids).
- Keyed mode **cannot define `count` at all** — the spec is rejected at parse
  time with `task <name> is stateful keyed mode and cannot define count: keyed
  instance cardinality is driven by the instance management API (legion
  instance create/destroy), not the job spec`. There is no fixed pool to size:
  the set of live instances *is* the cardinality.
- `initialInstances` is valid only in keyed mode; each id must match
  `[A-Za-z0-9._-]{1,128}` and the list must contain no duplicates.
- `storage.path` must be a non-empty absolute path.
- `storage.retainOnStop` is only valid in slot mode: keyed stop/destroy
  semantics are fixed per instance (instance stop / job stop always retains
  the handle, instance destroy always drops it), so the knob is rejected on
  keyed tasks with a validation error.
- At most one slot-mode stateful task per group (slot ids are derived from
  `(job, group, slot)`). Multiple keyed tasks per group are fine — their ids
  are explicit keys, not ordinals.
- `onLost` durations must parse (humantime strings like `'90s'`, `'3m'`);
  `selfTerminateAfter` must be greater than zero; `replaceAfter` must be
  **greater than** `selfTerminateAfter` (or the special value `'never'`) —
  otherwise a replacement could start before the worker has provably
  self-fenced. `onLost` belongs *inside* the `stateful` block; declaring it
  at the task level — including on a stateless task, which has nothing to
  fence — is rejected at parse time with a pointer to the right nesting.

### Seed list (`initialInstances`) semantics

`stateful.initialInstances` (keyed mode only) is a **seed list, not a
reconciled desired set**. On every submit, after the config is persisted and
before placement is evaluated, the manager ensures each listed id exists:

- Ids that have **never existed** are created (`pending`) and flow into the
  submit's normal placement eval.
- Ids that already exist in **any** state — live, `stopped`, or a `terminated`
  tombstone (even under another job) — are skipped silently. Resubmits are
  therefore idempotent, and a deliberately **destroyed id is never
  resurrected** by appearing in the seed list.

Because it is a seed and not a desired set, **removing** an id from the list on
a resubmit does *not* stop or destroy that instance — only `legion instance
stop`/`destroy` (or a job stop/destroy) changes a live instance's lifecycle.
Think of it as "create these on first run if they don't already exist", not
"the set of instances that should exist".

## Environment Variables

Injected by the manager into the task's runtime config `env` map at dispatch
time, so they are present at process start for `exec` and `container` tasks:

| Variable | When | Meaning |
| --- | --- | --- |
| `LEGION_ALLOC_ID` | **all** tasks (stateless too) | Current allocation id; churns each placement. |
| `LEGION_INSTANCE_ID` | stateful tasks | Stable instance id; survives reschedules. |
| `LEGION_INSTANCE_MODE` | stateful tasks | `slot` or `keyed`. |
| `LEGION_INSTANCE_SLOT` | slot mode only | Ordinal slot index `0..count-1`. |
| `LEGION_STORAGE_DIR` | tasks with `stateful.storage` | The instance volume's path. See [volumes.md](volumes.md#how-path-works-per-task-runtime) for why exec and container tasks get different values. |
| `LEGION_VOLUME_<NAME>` | per declared volume | Same, for every volume including `volumes[]` claims. |
| `LEGION_CLAIM_ID` | provisional starts only | The claim a workload was started under, before its placement was durable ([edge-boot.md](edge-boot.md)). Such a task sees **no** `LEGION_ALLOC_ID` — it does not have one yet, and a placeholder would be a lie. `LEGION_INSTANCE_ID` is correct from the first millisecond. |

Apps key their durable state and "who am I" decisions on
`LEGION_INSTANCE_ID`; `LEGION_ALLOC_ID` is the ephemeral placement id.

**Precedence:** Legion-injected variables win over user-declared env keys of
the same name. They are authoritative runtime facts — letting a jobspec shadow
them would let a workload misreport its own identity (this mirrors Nomad,
whose runtime variables always take precedence).

## Reschedule Semantics

When a slot's allocation dies (process failure, node loss, drain):

1. The instance transitions `active -> rescheduling`; its `instance_id` and
   `storage_handle` are untouched.
2. The scheduler plans a replacement allocation as usual (constraints,
   resources, scoring — slot identity does not influence node choice in v1).
3. The replacement is bound to the lowest free slot — i.e. the one that just
   lost its allocation — so it reuses the same instance id and storage handle,
   and the instance transitions back to `active`.

Config updates behave the same way: instances are keyed by *job* (not job
config version), so replica N keeps its identity and storage across deploys.

**Node loss is the exception**: a dead *process* is reported by a live
worker and replaced immediately, but a dead *heartbeat* only proves the
manager can't reach the node — the workload may still be running. Stateful
allocations on lost nodes are therefore gated behind a loss-confirmation
window before step 1 happens at all; see the next section.

### Preemption

A stateful allocation on **cluster-reachable** storage (`nfs`, and future
network drivers) is preemptible. Preemption stops it exactly the way a rollout
replacement does — a clean, manager-driven stop with the task's `stop` ladder —
and its replacement reattaches the same storage handle wherever it lands, by
the same reschedule path above.

A stateful allocation on **node-local** storage (`local-dir`) is **never** a
preemption victim, categorically. Evicting one either frees nothing (the
instance can only be recreated on the node its data is on) or abandons the
data. The rule follows the same principle as drain's held-stateful behavior:
the platform never force-moves protected state. The consequence to plan around
is that a node filled with low-band, node-pinned stateful work is
un-preemptible; the placement error says so. See
[preemption-and-eviction.md](preemption-and-eviction.md).

## Node loss and fencing (at-most-one)

Single-writer systems need certainty that slot N's old allocation is dead
before a replacement binds slot N's identity, storage handle, and
workload-identity `sub`. A missed heartbeat alone cannot give that
certainty: a network partition looks exactly like a dead node, and eagerly
replacing a partitioned node's allocation would run the same instance twice
(split-brain). Legion closes this with **lease-based fencing**: two
cooperating timers, one on each side of the partition.

1. **Worker self-fence** (`onLost.selfTerminateAfter`, default `90s`): if a
   worker cannot complete a heartbeat against the manager for longer than
   this window, it kills its stateful allocations locally (the heartbeat
   loop itself never exits on failure — it retries with backoff forever
   while an independent watchdog tracks the time since the last *successful*
   heartbeat). The fenced allocation is recorded as failed, so the first
   post-reconnect heartbeat reports it failed/stopped rather than running.
2. **Manager confirmation gate** (`onLost.replaceAfter`, default
   `2 x selfTerminateAfter`): a stateful allocation on a lost node is **not**
   freed for replacement until the node has been lost longer than this. The
   node row records `lost_at` when it is marked lost (cleared when it
   heartbeats again or re-registers), and the same monitor loop that marks
   nodes lost re-checks gated allocations every tick, so replacement
   proceeds automatically — no operator action — once the window elapses.

**Why this confirms the loss**: validation enforces
`replaceAfter > selfTerminateAfter`. By the time the manager frees the
allocation, one of two things is true on the unreachable node: the worker is
alive-but-partitioned and has already self-fenced (its window expired
strictly earlier, by the manager's own clock plus the safety margin), or the
worker is truly dead and so is the workload. Either way the replacement that
re-binds the instance can never run concurrently with the original — at most
one live allocation per instance, even across partitions.

While gated, nothing visibly changes: the allocation row stays put, the
instance stays `active` and bound, and `legion instance get/list` shows a
`pending-loss-confirmation` detail naming the lost node and what would
unblock it. Stateless tasks are untouched by all of this — they carry no
fencing window and are replaced eagerly the moment their node is marked
lost, exactly like today.

One bound to be aware of: a **config update** deliberately replaces every
allocation, including ones on an unreachable node that cannot be told to
stop. If you deploy while a node is partitioned, the outgoing allocation
there may overlap its replacement for up to `selfTerminateAfter` (the
worker's self-fence still bounds it — that lease is exactly why the window
exists). Prefer not deploying stateful jobs while nodes are lost, or size
`selfTerminateAfter` to an overlap your workload tolerates.

### Defaults, tuning, and `'never'`

Every stateful task gets the policy — an absent `onLost` block means
`selfTerminateAfter: '90s'`, `replaceAfter: '180s'`. **Safe by default
means stateful failover is deliberately slower than stateless failover**: a
replica on a lost node waits out the confirmation window (~3 minutes by
default) where a stateless task moves in seconds. For workloads where fast
failover matters more than a long partition grace (and whose checks/restarts
tolerate it), tune the *worker* side down — e.g. `selfTerminateAfter: '15s'`
lets `replaceAfter` default to 30s. The worker pays for that with less
tolerance of manager downtime: any manager outage longer than the window
kills the task locally (it is rescheduled when the manager returns).

`replaceAfter: 'never'` is the K8s StatefulSet behavior: the manager never
auto-replaces the allocation. Recovery requires the node coming back (the
allocation simply keeps running; `lost_at` clears) or an operator override.

### Operator override

`legion instance replace <id>` (RPC `ReplaceInstance`, MCP
`legion_instance_replace`; supports `--idempotency-key`) is the analog of
force-deleting a pod stuck on a lost K8s node: it frees the gated allocation
immediately — skipping the rest of the window, and despite `'never'` — and
enqueues a reschedule that excludes the lost node, so the replacement
re-binds the same instance id and storage handle elsewhere. It refuses to
act unless the instance's allocation actually sits on a `lost` node. Use it
only when the node is confirmed dead or isolated: forcing while the node is
merely partitioned and the worker has not yet self-fenced is exactly the
split-brain the gate exists to prevent.

### State walkthrough

`db-default-2` is `active` on `node-w3` with the default policy
(self-fence 90s, replace 180s) when `node-w3` drops off the network:

| t | manager view | worker view (partitioned) |
| --- | --- | --- |
| 0s | heartbeats from `node-w3` stop arriving | heartbeats to the manager start failing; retry with backoff |
| ~10s | monitor marks `node-w3` lost, sets `lost_at`; stateless allocations replaced now; `db-default-2` gated (`pending-loss-confirmation` in `instance get`) | task still running; self-fence clock at ~10s |
| 90s | still gated (90 < 180) | watchdog kills the allocation: `self-fencing allocation …: lost manager contact for 90s` |
| 180s+ | monitor tick confirms the loss: allocation freed, instance `rescheduling`, replacement planned on a live node, instance `active` again with the **same id and storage handle** | nothing running |
| later | if `node-w3` reconnects, its heartbeat reports the fenced allocation as failed and clears `lost_at`; the manager treats it as a normal dead allocation (it may already have been replaced) | worker rejoins cleanly |

Had the node returned before 180s, the gate would simply release: the
allocation (already self-fenced at 90s, or still running if the outage was
shorter than 90s) is reported by the returning worker and normal
reconciliation takes over — an allocation that never stopped keeps running
with its binding intact, and no replacement is ever planned.

## Rollouts and the single-live-allocation invariant

The "at most one live allocation per instance" invariant must hold even during
a rollout. The only rollout strategy that would break it is `createFirst`
(replace-before-stop), which starts the incoming allocation *before* stopping
the outgoing one — for a stateful task that means two live allocations sharing
one instance id, storage handle, and workload-identity subject at the same
time. The node-loss fencing described in [Node loss and fencing
(at-most-one)](#node-loss-and-fencing-at-most-one) cannot help here — a
createFirst overlap is deliberate and on healthy nodes, not a partition
artifact (see [stateful-vs-nomad-k8s.md](../internal_docs/stateful-vs-nomad-k8s.md)).

Therefore **a stateful task may not use a `createFirst` rollout**: the combo is
rejected at job admission with an actionable error pointing at this invariant.
Stateful groups must use the default stop-then-start (`destroyFirst`)
replacement, which guarantees the single-live-allocation invariant by
construction. (This is a temporary restriction until instance fencing exists.)

### Edge boot does not weaken the invariant

[Edge boot](edge-boot.md) lets a node start a keyed sandbox *before* its
placement is durable, which sounds like it should threaten the same invariant.
It does not, because an unratified start binds nothing: no instance id, no
storage handle, no workload identity. Binding still happens in exactly one
place — `assign_stateful_instance_tx`, inside the plan apply — and a
ratification is an ordinary plan submission that names the instance it is
racing for.

Two nodes that both boot `user-42` therefore resolve the same way two
concurrent plans resolve today: the store serializes them, the first binds, and
the second is refused with `instance_claimed` and kills its work. The existing
suites in this section run unchanged with `speculativeStart` on and off.

## Ordered rollouts for slot pools

Slot pools are ordinal by nature, and quorum systems care about the order in
which replicas restart. Two rollout options make config updates
ordinal-aware (`rollout.slots`, also scopeable per group via
`rolloutByGroup.<group>.slots` like every other rollout option):

```jsonnet
rollout: {
  slots: {
    order: 'sequential',   // 'parallel' (default, today's behavior) | 'sequential'
    updateFrom: 2,         // optional staged floor; requires order: 'sequential'
  },
}
```

### `order: 'sequential'` — one slot at a time, highest first

A sequential rollout replaces allocations **one slot per step, highest
ordinal first** (`count-1 → 0`), and each step is gated on the previous
slot's replacement passing the rollout's existing health criteria
(allocation health snapshots plus `minHealthyTime`, exactly like any other
rollout step). This is the update-order analog of a K8s StatefulSet's
`OrderedReady` pod management: followers (high ordinals) restart before the
conventional leader at slot 0.

- **Replacement is stop-then-start per slot**: the slot's old allocation is
  retired, its replacement is placed, **re-binds the same instance id and
  storage handle**, and must become healthy before the next (lower) slot is
  touched. (`createFirst` remains rejected for stateful groups, so the
  at-most-one invariant holds at every step.)
- **Halt on unhealthy**: if a replacement never passes the health gate, the
  rollout stops at that slot — lower slots keep running the old config.
  This inherits the standard rollout failure semantics: without a
  `healthyDeadline` the rollout waits (status `running`, with a
  `waiting for slot N replacement to become healthy` event); with one, the
  configured `onHealthDeadline` action (`rollback` by default) fires.
- **Legibility**: rollout events name each step's slot (`retired old
  allocation for slot 2`, `slot 2 updated and healthy`), and
  `GetRolloutStatus` reports `step_index`/`total_steps` where total steps =
  the number of slots being updated.
- Sequential defines its own one-slot-per-step progression, so it cannot be
  combined with the step modes (`percentEachStep`, `countEachStep`,
  `percentSteps`, `countSteps`) in the same scope. A per-group `slots`
  override replaces a global step mode (and vice versa), mirroring how step
  mode overrides already work. `progression: manual` composes fine: each
  manual advance releases one slot.

**Initial bring-up is not yet gated.** A fresh job's slots place in
ascending order (0 → N-1) by construction, but in a single eval without
per-slot health gating — the K8s `OrderedReady` *bring-up* analog is a
follow-up. `order: 'sequential'` governs config updates (and staged
promotions) today.

### `updateFrom` — staged ("partitioned") rollouts

`updateFrom: N` stages the rollout: **only slots `>= N` are updated to the
new config; slots `0..N` keep running their current allocations untouched**
— including across later reschedules (the scheduler knows staged slots are
deliberately occupied by the older config and will not double-place them).
This is the K8s StatefulSet `rollingUpdate.partition` analog and enables the
canary-promote workflow:

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

Validation rules (all at `job validate`/submit time, with actionable
errors):

- `rollout.slots` (both `order` and `updateFrom`) is only valid for groups
  whose stateful task is **slot mode** — stateless and keyed groups are
  rejected, with a pointer at `rolloutByGroup` scoping for mixed jobs.
- `updateFrom` requires `order: 'sequential'`. (K8s `partition` exists only
  inside ordered rolling updates; "update slots >= N" has no coherent
  meaning unless slots are replaced by ordinal.)
- `updateFrom` must be `< count.num`. K8s permits `partition >= replicas`
  to mean "update nothing", but a rollout that completes instantly while
  silently pinning every slot to the old config is almost always a typo in
  Legion — omit the field to update everything, or lower it to stage.
- `updateFrom: 0` is valid and identical to omitting the field.
- Negative or non-numeric values and unknown `order` values are rejected at
  parse time.

### K8s mapping

| K8s StatefulSet | Legion |
| --- | --- |
| `podManagementPolicy: OrderedReady` (update order) | `rollout.slots.order: 'sequential'` |
| `updateStrategy.rollingUpdate.partition` | `rollout.slots.updateFrom` |
| `podManagementPolicy: Parallel` | `order: 'parallel'` (the default) |
| OrderedReady bring-up gating | not yet (slots place 0→N-1 but ungated) |

## Keyed Mode: the Management API

Keyed mode is the sandbox/dispatch primitive: 0..thousands of long-lived
logical instances per (job, group, task), created and destroyed on demand,
each with a unique id that is **never recycled for the lifetime of the job**.

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
  The scheduler derives a keyed task's desired allocation count from its live
  (pending/active/rescheduling) instances — keyed tasks define no `count`, and a
  keyed task with zero instances runs zero allocations. Stopped instances are
  NOT in the desired set.
- **Placement** binds the new allocation to an unbound live instance inside
  the same transaction that inserts the allocation (the same choke point slot
  mode uses), and mints the storage handle on first placement if the task
  declares `stateful.storage`.
- **Failure** of the allocation moves the instance to `rescheduling`; the
  replacement allocation re-binds the *same* instance id and storage handle.
  Allocation churn never terminates or stops a keyed instance.
- **Stop** (`StopInstance` / `legion instance stop`) pauses the instance:
  stops its allocation through the normal allocation-stop path and transitions
  the row to `stopped`, **retaining the row and its storage handle**. The id
  stays owned (creates still collide) and the instance is not scheduled until
  started. See [Stopping and starting](#stopping-and-starting).
- **Start** (`StartInstance` / `legion instance start`) reverses a stop:
  `stopped -> pending`, re-placed against the same id and retained handle.
- **Destroy** stops the instance's allocation through the normal
  allocation-stop path, **drops the storage handle**, and retains the row as a
  `terminated` tombstone for the lifetime of the job (destroying the job clears
  it).

### API / CLI

Four manager RPCs (`CreateInstance`, `DestroyInstance`, `ListInstances`,
`GetInstance` — see `proto/legion.proto` for the authoritative contract),
surfaced as `legion instance ...` and as MCP tools
(`legion_instance_create/destroy/stop/start/list/get`):

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
instance, stopping an already-stopped one, and starting an already-live one all
succeed as no-ops. `--group`/`--task` are only needed when the job declares
more than one keyed task; ambiguity is rejected, never guessed.

### Stopping and starting

`stop` and `start` (keyed mode only — slot lifecycles are owned by the
scheduler via `count.num`) make a keyed instance a *reversible pause*, distinct
from the irreversible `destroy`:

| | `stop` | `destroy` |
| --- | --- | --- |
| Row | retained (`stopped`) | retained (`terminated` tombstone) |
| Storage handle | **retained** | **dropped** |
| Id | stays owned (creates collide) | stays owned for the job's lifetime |
| Reversible | yes, via `start` | no — create a new id instead |
| In desired set | no (not scheduled until started) | no, ever |

- **Stop** removes the instance's allocation in the same transaction that flips
  the row to `stopped`, so the release hook can never race it back to
  `rescheduling`. While stopped the instance is not part of the keyed task's
  desired count: resubmits, config updates, and reschedules will not resurrect
  it. Only `start` does.
- **Start** transitions `stopped -> pending` and enqueues placement; the new
  allocation re-binds the same instance id and its retained storage handle, so
  the revived sandbox is *the same* logical instance with its data.
- **Terminated** (destroyed) instances cannot be stopped or started — the id is
  tombstoned for the job's lifetime; create a new id instead.

Revival walkthrough (stop, then bring it back):

```bash
legion instance create sandbox-demo --id user-42   # active
legion instance stop user-42                        # paused, handle kept
legion instance get user-42 -j                      # state: "stopped"
legion instance start user-42                        # re-placed, same handle
legion instance get user-42 -j                      # state: "active" again

# A job stop is a bulk pause for keyed instances:
legion job stop sandbox-demo                         # keyed -> stopped (handles kept)
legion job run configs/jobs/sandbox-demo.jsonnet     # job revived...
legion instance list sandbox-demo -j                 # ...but instances still "stopped"
legion instance start user-42                        # each one revived explicitly
```

Note that resubmitting a stopped job (`job run`) clears the *job's* stopped
status but **does not** auto-start its stopped keyed instances — each needs an
explicit `instance start`. This keeps revival intentional (you choose which
sandboxes come back).

### Id rules

- Charset `[A-Za-z0-9._-]`, length 1–128 (ids flow into env vars, workload
  identity subjects, and CLI args).
- `stateful.idSource` declares the *default* source: `'caller'` makes the id
  required on create; `'uuid'` (or unset) lets the server mint one. A valid
  caller-supplied id is always honored regardless of `idSource`.
- Ids are unique within the job and **never recycled for the lifetime of the
  job**: creating an id that collides with any existing row of the job —
  including a `stopped` instance or a `terminated` tombstone — is rejected
  (`AlreadyExists`). So a dead sandbox's identity (and its WI `sub`) can never
  be handed to a new one while the job lives. Destroying the whole job
  (`job destroy`) cascades and clears its instance history (rows are deleted
  with the job, see [Scale-In / Scale-Out Retention](#scale-in--scale-out-retention)),
  after which an id from that job is free again — the never-recycle guarantee
  is scoped to the job's lifetime, not eternal.
- Keyed ids may not collide with the reserved slot-id pattern
  `{job}-{group}-{slot}` of a slot-mode stateful task **in the same job**: the
  slot and keyed id spaces are kept disjoint so a keyed id can never adopt, or
  be adopted by, a slot placement (a create supplying such an id is rejected,
  and a slot binding never adopts a foreign row).

### Storage on destroy

Destroy always drops the storage handle. This is deliberately the opposite of
slot scale-in (which retains storage by default for a later scale-out): a
slot id will run again and must reattach its data; a destroyed keyed id can
never run again, so letting its state remain attachable would be a leak.

Stopping the whole job (`job stop`) is **not** a destroy: keyed instances are
moved to `stopped` (not `terminated`) with their storage handles **retained**,
so `job run` + `instance start` revives them with their data. Slot instances
become `terminated` with handles retained too (unless the task sets
`retainOnStop: false`, see [Scale-In / Scale-Out
Retention](#scale-in--scale-out-retention)), so a later `job run`
re-materializes the same ordinal identities. Only `instance destroy` / `job
destroy` drops keyed handles.

### Count interaction

Keyed tasks have no `count` — defining one is a parse-time error (see the
validation rules above), because cardinality is owned entirely by the
instance management API. Submitting the job places nothing until instances
are created, and config updates/rollouts converge each keyed task to its
live-instance count, replacing every live instance's allocation on the new
config.

## Scale-In / Scale-Out Retention

- **Scale-in** (`count` 3 → 2): slot 2's instance is marked `terminated` but
  the **row is retained**, and its storage handle is kept by default
  (`retainOnScaleIn: true`). With `retainOnScaleIn: false` the handle is
  dropped on scale-in (reclamation itself is a driver concern, see below).
- **Scale-out** (2 → 3): slot 2 is re-materialized with the **same instance
  id**, and a retained storage handle is reattached. If no handle was retained
  a fresh one is minted.
- **Job stop**: rows are retained for all instances. Slot instances become
  `terminated`, so a later `job run` re-materializes the same identities;
  their handles are retained by default (`retainOnStop: true`) so the re-run
  reattaches the same data, while `retainOnStop: false` drops the handles on
  stop and the revived slots mint fresh ones — identity comes back, the data
  binding doesn't. Keyed instances become `stopped` (handles retained,
  always), so `job run` + `legion instance start <id>` revives each one with
  its data (see [Stopping and starting](#stopping-and-starting)). A stop is a
  pause, not a destroy.
- **Job destroy**: instance rows are deleted with the job (FK cascade),
  storage handles included, regardless of `retainOnStop` — that knob governs
  `job stop` only. This is the only thing that clears a job's keyed-id
  history — the never-recycle guarantee holds for the lifetime of the job,
  not beyond it.

### K8s mapping

The two knobs are the slot-mode analog of a StatefulSet's
`persistentVolumeClaimRetentionPolicy` (both default Retain):

| K8s | Legion |
| --- | --- |
| `whenScaled: Retain/Delete` | `retainOnScaleIn: true/false` |
| `whenDeleted: Retain/Delete` | `retainOnStop: true/false` (`job stop`; `job destroy` always deletes) |

`whenDeleted` maps to `job stop` — not `job destroy` — because stopping is
Legion's analog of deleting a StatefulSet (the spec lingers, identities can
come back), whereas `job destroy` purges the job's entire record history
unconditionally.

## Storage Handle Contract

Each instance optionally owns a `storage_handle`, minted on first placement
when the task declares `stateful.storage`. It is a `{plugin}/{volume_id}` pair
(`local-dir/9f2c…`): which driver owns the storage, and which volume. The
contract is:

- the handle binds to the **instance**, never the allocation;
- every allocation for the instance sees the same handle;
- handles survive reschedules, config updates, and (by default) scale cycles.

Behind the handle is a real volume, materialized by a plugin on whatever node
the instance lands on: mounted before the task starts, unmounted on stop, and
reclaimed when the handle is dropped. Node-local volumes additionally **pin**
their instance to the node holding the data, so a replacement can never start on
an empty disk. See **[docs/volumes.md](volumes.md)** for plugins, the driver
contract, and `legion volume`.

Every task runtime consumes the same machinery — there is no runtime-private
storage. What differs is the *shape* a runtime takes
([`volume_mode`](volumes.md#filesystem-mode-vs-block-mode)): `exec` and
`container` consume a host directory, while the
[`sandbox` runtime](task-runtime-sandbox.md) declares `mode: 'block'` and is handed
a formatted disk image its microVM attaches as a virtio-blk device. Either
way, single-writer safety is the invariant above: a filesystem (or a
filesystem image) tolerates exactly one writer, and "at most one live
allocation per instance" is precisely that guarantee.

## Workload Identity Integration

The instance id is the stable key the OIDC workload identity consumes (see
[workload-identity.md](workload-identity.md)): stateful claims carry
`legion.io/instance` (and `legion.io/slot` in slot mode), and `sub` templates
on stateful tasks may use the `{instance}` / `{slot}` placeholders. Because
the instance id survives reschedules, the token's identity is stable even
though each allocation mints fresh short-lived tokens.

## Trying It Locally

Start a manager, join a worker, and run the demo job:

```bash
# Terminal 1: manager
cargo run -- start manager -c configs/manager.jsonnet

# Terminal 2: worker (use the join token printed by the manager)
cargo run -- join <TOKEN>

# Terminal 3: run the slot-mode demo (count 3, stateful storage)
cargo run -- job run configs/jobs/stateful-demo.jsonnet
cargo run -- job list -j
```

Each replica logs its identity at start, e.g.:

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

Kill the worker process hosting slot 1 (or stop its allocation) and watch the
replacement come up with the **same** `LEGION_INSTANCE_ID` and slot but a new
`LEGION_ALLOC_ID`. Instance rows are visible via the state store:

```bash
cargo run -- state query 'SELECT instance_id, state, slot_index, current_alloc_id, storage_handle FROM instance' -j
```

### Keyed mode walkthrough

With the same manager + worker running:

```bash
# Submitting the keyed demo places ZERO allocations (count is not in charge)
cargo run -- job run configs/jobs/sandbox-demo.jsonnet
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

Tasks without a `stateful` block behave exactly as before: no instance rows
are created, allocations carry no `instance_id`, and no `LEGION_INSTANCE_*`
variables are injected. The only addition for stateless tasks is
`LEGION_ALLOC_ID`, which is universally useful and harmless. This regression
guarantee is covered by tests
(`stateless_job_plans_carry_no_instance_metadata`,
`stateless_jobs_create_no_instance_rows`,
`injects_alloc_id_for_stateless_tasks_without_instance_vars`).
