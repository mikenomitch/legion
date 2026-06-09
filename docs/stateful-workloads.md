# Stateful Workloads & Instance Identity

By default every Legion allocation is **anonymous and interchangeable** — when a
task instance dies and reschedules, the replacement gets a fresh `alloc_id` and
no memory of "which one" it was. Stateful workloads need the opposite: a
**stable, addressable, per-instance identity** that survives rescheduling so an
instance can reclaim its volume, register its peer address, or own its shard.

This document describes the instance-identity model. The design rationale is in
[docs/plans/stateful-workloads-and-instance-identity-plan.md](plans/stateful-workloads-and-instance-identity-plan.md).

> **Implementation status.** Delivered in phases. The current code implements
> the *foundation*: the `stateful` spec block, the `Instance` model with stable
> ids and DNS-style network names, slot-mode materialization (dense ordinal
> recycling), keyed-mode create/destroy/tombstone logic, the `instance` table +
> persistence, and the workload-identity bridge (an instance feeds the `sub` and
> `legion.io/instance` / `legion.io/slot` claims). Still to be wired (later
> phases in the plan): the scheduler reconcile loop that turns desired instances
> into allocations and stamps `instance_id` onto the allocation + worker env, the
> keyed-mode gRPC API, DNS/service discovery, and volume-affinity scheduling.

## The `stateful` block

Add an optional `stateful` block to a task. Absent = today's stateless,
fungible-allocation behavior, unchanged.

```jsonnet
{
  name: 'db',
  kind: 'container',
  count: 3,                 // slot mode: number of slots (db:data:0..2)
  stateful: {
    mode: 'slot',           // 'slot' | 'keyed' (default 'slot')
    stableNetworkId: true,  // deterministic DNS-style name per instance
    volumeAffinity: true,   // record instance→node binding hints
  },
}
```

A runnable example is at
[configs/jobs/stateful-db.jsonnet](../configs/jobs/stateful-db.jsonnet).

## Two identity modes

### Slot mode (`db-0`, `db-1`)

Dense, ordinal-based, Kubernetes StatefulSet-style. The scheduler maintains
instances `0..count-1`. Because the instance id is a pure function of the
ordinal, scaling **down then up recycles ordinals densely and deterministically**
— ordinal `N` is always the same instance id, so a revived slot reuses the same
identity (and volume/credentials), never a new one.

- `count = 3` → `db:data:0`, `db:data:1`, `db:data:2`.
- Scale to `1` → ordinals `1` and `2` marked undesired (rows retained).
- Scale back to `3` → ordinals `1`, `2` revived with the *same* ids.

### Keyed mode (`tenant-acme`, `sandbox-xyz`)

Sparse, caller-supplied stable keys. Instances are created/destroyed via API
(`count` is ignored — validation rejects a keyed task with an explicit count).
The key space is **operator-owned**, and **destroyed keys are tombstoned and
never recycled**: a destroyed key's identity (and any federated credentials
scoped to it) can never be silently reissued to a different instance.

## Stable instance id

The canonical id is `<job>:<group>:<slot-or-key>` (e.g. `db:data:0`,
`sb:main:tenant-acme`). This is exactly the stable handle the
[workload-identity](workload-identity.md) `sub` extension consumes:

```
sub = <region>:<job>:<group>:<task>:<instance>
```

Because the id is stable across reschedules, each reschedule mints a *fresh*
short-lived token carrying the *same* `sub` — identity persists, the token does
not. Use a per-identity `subTemplate` of
`{region}:{job}:{group}:{task}:{instance}` to opt a stateful task's tokens into
the extended subject; `{instance}` / `{slot}` are only valid on tasks that
declare a `stateful` block.

## Stable network identity

When `stableNetworkId` is set, each instance gets a deterministic name:

- slot: `<group>-<ordinal>.<job>.<region>` (e.g. `data-1.db.auto`)
- keyed: `<key>.<job>.<region>` (e.g. `acme.sb.auto`)

recorded on the instance so a future DNS/service-discovery layer can resolve
peers consistently across reschedules.

## Volume affinity

When `volumeAffinity` is set, the instance records the `node_id` it last ran on.
A future scheduler phase treats this as a strong (not absolute) preference so an
instance returns to its data on reschedule; actual volume binding is delegated
to a future storage plugin via this metadata.

## Persistence

Instances live in their own `instance` table — a new persistent layer *between*
the task spec and allocations: the spec says "I want N slots (or these K keys)",
the table records each instance's stable identity, desired state, tombstone
marker, and current allocation, and the scheduler reconciles
instances→allocations the way it already reconciles count→allocations. The
table (and a task's `stateful` config) survive manager restarts.
