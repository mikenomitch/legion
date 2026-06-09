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

- **`keyed`** (sandbox / dispatch style) — spec parses and validates today;
  the create/destroy management RPCs are **in progress** (a follow-up
  workstream). Instances are created on demand, each with a unique,
  **never-recycled** id, so a new sandbox can never inherit a dead one's
  identity or persisted state.

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
      mode: 'slot',              // 'slot' | 'keyed' (keyed mgmt API in progress)
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
- `idSource` is only valid in keyed mode.
- Slot mode requires `count.num >= 1` and cannot be combined with `count.per`
  (per-attribute pools would collide on slot ids).
- `storage.path` must be a non-empty absolute path.
- At most one slot-mode stateful task per group (slot ids are derived from
  `(job, group, slot)`).

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

## Scale-In / Scale-Out Retention

- **Scale-in** (`count` 3 → 2): slot 2's instance is marked `terminated` but
  the **row is retained**, and its storage handle is kept by default
  (`retainOnScaleIn: true`). With `retainOnScaleIn: false` the handle is
  dropped on scale-in (reclamation itself is a driver concern, see below).
- **Scale-out** (2 → 3): slot 2 is re-materialized with the **same instance
  id**, and a retained storage handle is reattached. If no handle was retained
  a fresh one is minted.
- **Job stop**: all instances are terminated with rows + handles retained; a
  later `job run` re-materializes the same identities.
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

## Stateless Jobs Are Unaffected

Tasks without a `stateful` block behave exactly as before: no instance rows
are created, allocations carry no `instance_id`, and no `LEGION_INSTANCE_*`
variables are injected. The only addition for stateless tasks is
`LEGION_ALLOC_ID`, which is universally useful and harmless. This regression
guarantee is covered by tests
(`stateless_job_plans_carry_no_instance_metadata`,
`stateless_jobs_create_no_instance_rows`,
`injects_alloc_id_for_stateless_tasks_without_instance_vars`).
