# Stateful Workloads & Instance Identity

Legion's scheduling unit is the **allocation** — an ephemeral placement of a
task on a node. When a task is rescheduled (node loss, drain, config update)
it gets a *new* allocation with a *new* id. Stateful workloads need a handle
that survives that churn: replica N must always come back as replica N, with
replica N's data.

The `stateful` task block introduces that handle: the **instance**.

> Design plan: [stateful-workloads-and-instance-identity-plan.md](plans/stateful-workloads-and-instance-identity-plan.md).
> Related: [workload-identity.md](workload-identity.md) (the instance id feeds
> the reschedule-stable token subject).

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
  `CreateInstance`), each with a unique, **never-recycled** id, so a new
  sandbox can never inherit a dead one's identity or persisted state.
  `count.num` does not drive keyed tasks: the set of live instances does
  (zero instances = zero allocations). See
  [Keyed Mode: the Management API](#keyed-mode-the-management-api).

### Instance lifecycle

```
pending -> active -> rescheduling -> active -> ... -> terminated
```

- `pending` — row exists, never placed.
- `active` — a live allocation fulfills it (`current_alloc_id` points at it).
- `rescheduling` — its allocation died; the replacement will reuse the same
  instance id and storage handle.
- `terminated` — no longer desired (scaled in or job stopped). **Rows are
  retained**: slot rows so a later scale-out re-materializes the same id,
  keyed rows forever (the never-recycle guarantee).

## Job Spec Reference

```jsonnet
{
  name: 'db',
  tasks: [{
    name: 'replica',
    kind: 'exec',
    count: 3,                    // slot cardinality: slots 0, 1, 2
    stateful: {
      mode: 'slot',              // 'slot' | 'keyed'
      // idSource: 'caller',     // keyed only: 'caller' | 'uuid'
      storage: {                 // optional persistent-state contract
        path: '/var/lib/db',     // absolute path the state is exposed at
        retainOnScaleIn: true,   // default true (K8s PVC-style retention)
      },
    },
    config: { command: ['run-db'] },
  }],
}
```

Validation rules (enforced at `job validate` / submit time):

- `mode` is required (`slot` or `keyed`).
- `idSource` is only valid in keyed mode (`'caller'` or `'uuid'`).
- Slot mode requires `count.num >= 1` and cannot be combined with `count.per`
  (per-attribute pools would collide on slot ids).
- Keyed mode cannot be combined with `count.per`; `count.num` is **ignored**
  for keyed tasks (cardinality is driven by the instance management API).
- `storage.path` must be a non-empty absolute path.
- At most one slot-mode stateful task per group (slot ids are derived from
  `(job, group, slot)`). Multiple keyed tasks per group are fine — their ids
  are explicit keys, not ordinals.

## Environment Variables

Injected by the manager into the task's kind-config `env` map at dispatch
time, so they are present at process start for `exec` and `container` tasks:

| Variable | When | Meaning |
| --- | --- | --- |
| `LEGION_ALLOC_ID` | **all** tasks (stateless too) | Current allocation id; churns each placement. |
| `LEGION_INSTANCE_ID` | stateful tasks | Stable instance id; survives reschedules. |
| `LEGION_INSTANCE_MODE` | stateful tasks | `slot` or `keyed`. |
| `LEGION_INSTANCE_SLOT` | slot mode only | Ordinal slot index `0..count-1`. |

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

## Keyed Mode: the Management API

Keyed mode is the sandbox/dispatch primitive: 0..thousands of long-lived
logical instances per (job, group, task), created and destroyed on demand,
each with a unique id that is **never recycled**.

### Lifecycle

```
CreateInstance ──> pending ──placed──> active ──alloc dies──> rescheduling
                                         ^                          │
                                         └────replacement re-binds──┘
                          DestroyInstance / job stop ──> terminated (tombstone, forever)
```

- **Create** registers the instance (state `pending`) and enqueues placement.
  The scheduler derives a keyed task's desired allocation count from its live
  (pending/active/rescheduling) instances — `count.num` is ignored, and a
  keyed task with zero instances runs zero allocations.
- **Placement** binds the new allocation to an unbound live instance inside
  the same transaction that inserts the allocation (the same choke point slot
  mode uses), and mints the storage handle on first placement if the task
  declares `stateful.storage`.
- **Failure** of the allocation moves the instance to `rescheduling`; the
  replacement allocation re-binds the *same* instance id and storage handle.
  Allocation churn never terminates a keyed instance.
- **Destroy** stops the instance's allocation through the normal
  allocation-stop path, **drops the storage handle**, and retains the row
  forever as a `terminated` tombstone.

### API / CLI

Four manager RPCs (`CreateInstance`, `DestroyInstance`, `ListInstances`,
`GetInstance` — see `proto/legion.proto` for the authoritative contract),
surfaced as `legion instance ...` and as MCP tools
(`legion_instance_create/destroy/list/get`):

```bash
# create with an explicit key (idSource 'caller' requires one; with 'uuid'
# or unset the server mints an id when --id is omitted)
legion instance create sandbox-demo --id user-42 -j
# => { "ok": true, "data": { "instanceId": "user-42", "deduplicated": false } }

legion instance list sandbox-demo -j
# => { "ok": true, "data": [ { "instanceId": "user-42", "jobId": "sandbox-demo",
#      "group": "sandboxes", "task": "sandbox", "mode": "keyed",
#      "state": "active", "currentAllocId": 7,
#      "storageHandle": "local-dir/…", "createdAt": "…", "updatedAt": "…" } ] }

legion instance get user-42 -j

legion instance destroy user-42 -j
# => { "ok": true, "data": { "message": "Destroyed instance user-42",
#      "deduplicated": false } }
```

`create` and `destroy` accept `--idempotency-key` and report `deduplicated`,
like the other mutating RPCs. Destroying an already-terminated instance
succeeds as a no-op. `--group`/`--task` are only needed when the job declares
more than one keyed task; ambiguity is rejected, never guessed.

### Id rules

- Charset `[A-Za-z0-9._-]`, length 1–128 (ids flow into env vars, workload
  identity subjects, and CLI args).
- `stateful.idSource` declares the *default* source: `'caller'` makes the id
  required on create; `'uuid'` (or unset) lets the server mint one. A valid
  caller-supplied id is always honored regardless of `idSource`.
- Ids are unique cluster-wide and **never recycled**: creating an id that
  collides with any existing row — including a `terminated` tombstone — is
  rejected (`AlreadyExists`). A dead sandbox's identity (and so its WI `sub`)
  can never be handed to a new one.

### Storage on destroy

Destroy always drops the storage handle. This is deliberately the opposite of
slot scale-in (which retains storage by default for a later scale-out): a
slot id will run again and must reattach its data; a destroyed keyed id can
never run again, so letting its state remain attachable would be a leak.
Stopping the whole job likewise terminates keyed instances and drops their
handles (slot handles are retained).

### Count interaction

`count.num` does not drive keyed tasks — submitting the job places nothing
until instances are created, and config updates/rollouts converge each keyed
task to its live-instance count, replacing every live instance's allocation
on the new config. `count.per` is rejected at validation time for keyed
tasks.

## Scale-In / Scale-Out Retention

- **Scale-in** (`count` 3 → 2): slot 2's instance is marked `terminated` but
  the **row is retained**, and its storage handle is kept by default
  (`retainOnScaleIn: true`). With `retainOnScaleIn: false` the handle is
  dropped on scale-in (reclamation itself is a driver concern, see below).
- **Scale-out** (2 → 3): slot 2 is re-materialized with the **same instance
  id**, and a retained storage handle is reattached. If no handle was retained
  a fresh one is minted.
- **Job stop**: all instances are terminated with rows retained. Slot
  handles are retained too, so a later `job run` re-materializes the same
  identities; keyed handles are dropped (terminated keyed ids never run
  again — see [Storage on destroy](#storage-on-destroy)).
- **Job destroy**: instance rows are deleted with the job.

## Storage Handle Contract

Each instance optionally owns an opaque `storage_handle`, minted on first
placement when the task declares `stateful.storage` (currently a
`local-dir/<id>` token). The contract is:

- the handle binds to the **instance**, never the allocation;
- every allocation for the instance sees the same handle;
- handles survive reschedules, config updates, and (by default) scale cycles.

What v1 deliberately does **not** do: actually attach/mount anything, pin
node-affine handles to nodes, or garbage-collect dropped handles. Concrete
drivers (local dir, network volumes, attach-before-start/detach-on-stop
hooks) are specified in the
[volume plugin system plan](plans/volume-plugin-system-plan.md).

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

# Destroy: allocation stops, handle dropped, id tombstoned forever
cargo run -- instance destroy user-42
cargo run -- instance get user-42 -j           # state: "terminated", no handle
cargo run -- instance create sandbox-demo --id user-42
# => error: AlreadyExists — destroyed ids are never recycled
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
