# Plan: Stateful Workloads & Instance Identity

> **Judgement calls:** [`calls/stateful-workloads-and-instance-identity-plan_calls.md`](../calls/stateful-workloads-and-instance-identity-plan_calls.md)

> **Related:** This plan defines the stable *instance identity* primitive that
> [Workload Identity via OIDC](workload-identity-oidc-plan.md) consumes to issue a
> token whose subject persists across reschedules. Read them together: this doc owns
> *what a stable instance is and how its key/slot is assigned*; the WI doc owns *how
> that key becomes a signed token claim*.

## Problem Statement

Legion's scheduling unit today is the **allocation** — an ephemeral placement of a
task on a node. The `Allocation` record ([src/structs/ledger.rs:12](../../src/structs/ledger.rs))
carries a churning `id` plus `node_id`/`job_id`/`task_config_id`. When a task is
rescheduled (node failure, drain, rollout) it gets a **new** allocation with a **new**
id, on a possibly-different node. There is no stable handle that survives that move.

That breaks two classes of workload:

1. **Traditional stateful replicas** (databases, queues, anything with a sticky
   on-disk dataset): replica *N* must always reattach replica *N*'s storage, no matter
   which node it lands on.
2. **Sandbox-style workloads** (code sandboxes, per-user runtimes): each sandbox is a
   long-lived logical entity created on demand, persists state across allocations, and
   must never be confused with another sandbox.

Both need a **stable instance identity** that outlives any single allocation, binds to
persistent state, is exposed to the task (env vars), and feeds a stable workload
identity token. Legion has no such primitive today — no instance index, no slot, no
volume binding. This plan introduces it.

---

## Goals

- **Stable instance identity above the allocation.** Introduce an *instance* — a
  logical entity that persists across allocations and owns a stable ID. An allocation
  becomes "the current placement *of* an instance."
- **Two assignment modes, one identity model:**
  - **Slot mode** (StatefulSet / Nomad `per_alloc` style): a fixed-size group of
    instances with ordinal slots `0..count-1`; a replacement allocation for slot *N*
    reuses slot *N*'s id and storage.
  - **Keyed mode** (sandbox / dispatch style): instances are created on demand, each
    assigned a unique, **never-recycled** id at spin-up time, with an explicit
    create/destroy lifecycle.
- **State binding.** Persistent storage attaches to the instance id (slot or keyed),
  not the allocation — so reschedules reattach the same data.
- **Env exposure.** The instance id and (in slot mode) the slot index are injected
  into the task as environment variables, available at process start.
- **WI integration.** The instance id is the stable key the OIDC `sub` and
  `legion.io/instance` claim are built from (see the WI plan).
- **Safe recycling semantics.** Slot ids may be reused within a fixed pool; keyed ids
  must never be reused, so a new sandbox can never inherit a dead one's identity/state.

## Non-Goals

- **A full volume/CSI plugin design.** This plan defines the *binding* (instance →
  storage handle) and lifecycle hooks; the concrete storage backends (local dir,
  network volume, plugin ABI) are a follow-up. We specify the contract, not the driver.
- **Issuing or signing tokens.** That is the [WI plan](workload-identity-oidc-plan.md);
  here we only produce the stable key it consumes.
- **Cross-region instance migration / replication.** Instances are region-scoped.
- **Autoscaling policy for keyed instances.** Creation/destruction is driven by the
  API/caller; this plan provides the primitive, not the controller.

---

## Background: What Exists Today

| Concern | Where | Notes |
| --- | --- | --- |
| Allocation record | [src/structs/ledger.rs:12](../../src/structs/ledger.rs) | `id`, `node_id`, `job_id`, `task_config_id`. No stable instance handle. |
| Task group + count | `TaskConfig.group`, `TaskCount { num, per }` ([src/structs/job.rs:185](../../src/structs/job.rs)) | `count.num` is the natural source of slot cardinality. |
| Scheduler / placement | [src/manager/scheduler.rs](../../src/manager/scheduler.rs) | Where slot assignment + reuse must hook in. |
| Env injection | [src/task_plugins/exec.rs](../../src/task_plugins/exec.rs) (`cmd.envs`), [container.rs](../../src/task_plugins/container.rs) (`--env`) | Instance env vars ride this existing path. |
| State / migrations | [src/storage/migrations.rs](../../src/storage/migrations.rs), SQLite | Instance records persisted here. |
| Manager gRPC API | `proto/legion.proto` `service Manager` | Keyed-mode create/destroy RPCs added here. |

There is **no** volume, stateful, ordinal, or instance concept in the codebase today —
this is greenfield.

---

## Proposed Design

### 1) The Instance

A new persisted entity, conceptually one level above `Allocation`:

```
Instance {
  instance_id:  String,   // stable, unique within (job, group); survives reschedules
  slot_index:   Option<u32>, // Some(n) in slot mode; None in keyed mode
  job_id:       String,
  group:        String,
  mode:         InstanceMode, // Slot | Keyed
  state:        InstanceState, // Pending | Active | Rescheduling | Terminated
  current_alloc: Option<i64>,  // the allocation currently fulfilling it (if any)
  storage_handle: Option<String>, // opaque ref to bound persistent state
  created_at:   ...,
}
```

Invariants:
- An instance has **at most one** active allocation at a time; over its life it has
  many. Allocations gain an `instance_id` foreign key.
- `instance_id` is stable for the instance's whole life and is what state + identity
  bind to. `slot_index` is a *human/ordinal* convenience present only in slot mode.

### 2) Slot Mode (traditional stateful)

- Cardinality comes from `count.num`. The scheduler maintains slots `0..num-1`.
- Each slot has a deterministic `instance_id` derived from its ordinal, e.g.
  `<job>-<group>-<slot>` — so the id is recomputable and human-legible.
- A replacement allocation for slot *N* **reuses** slot *N*'s `instance_id` and
  storage handle. Reuse within a fixed pool is the desired behavior (replica 3's disk
  is replica 3's disk).
- Scaling down frees high slots; scaling back up re-materializes them with the same
  ids and storage. (Retention-on-scale-down is an open question, mirroring K8s PVC
  retention policy.)

### 3) Keyed Mode (sandbox-style)

- Instances are **created explicitly** via API, each assigned a unique
  `instance_id` (caller-supplied stable key, or server-minted UUID) at creation time.
- The id is **never recycled.** On destroy it transitions to `Terminated` and is
  garbage-collected; a new sandbox always gets a fresh id.
- Lifecycle is decoupled from a fixed `count`: there may be 0 or thousands of
  instances of the same (job, group), created/destroyed on demand. This is the
  parameterized/dispatch model (cf. Nomad parameterized jobs).
- `slot_index` is `None`; identity is purely the keyed id.

> ★ Insight ─────────────────────────────────────
> Slot reuse vs. never-recycle is the single most important safety distinction here.
> A recycled ordinal slot is *correct* for a replica set — replica 3 coming back must
> be replica 3. But for security-isolated sandboxes, recycling an id would hand a
> brand-new sandbox a dead one's identity (its WI `sub`) and its persisted state — a
> cross-tenant data/identity leak. Encoding the recycle policy *in the mode* makes the
> safe choice the default for each workload class instead of a footgun.
> ─────────────────────────────────────────────────

### 4) State Binding

- Each instance owns an opaque `storage_handle` allocated on first placement and
  reattached on every subsequent allocation for that instance.
- The task-launch pipeline mounts/links the handle before the process starts; on
  reschedule to a new node the same handle is reattached (semantics depend on the
  future storage driver — e.g. local-dir handles are node-local and constrain
  placement, network handles are not).
- This plan defines the *contract* (instance → handle, attach-before-start,
  detach-on-stop); concrete drivers are a follow-up (see Non-Goals).

### 5) Environment Variables

The instance identity is injected into the task via the existing env path
([src/task_plugins/exec.rs](../../src/task_plugins/exec.rs) / [container.rs](../../src/task_plugins/container.rs)),
so it's available at process start with no app-side API call (mirrors Nomad's
`NOMAD_ALLOC_INDEX`):

| Variable | Mode | Meaning |
| --- | --- | --- |
| `LEGION_INSTANCE_ID` | both | Stable instance id; survives reschedules. |
| `LEGION_INSTANCE_SLOT` | slot only | Ordinal slot index `0..count-1`. |
| `LEGION_ALLOC_ID` | both | Current allocation id (churns each placement). |
| `LEGION_INSTANCE_MODE` | both | `slot` or `keyed`. |

Apps use `LEGION_INSTANCE_ID` to key their durable state and to know "who am I"
consistently across restarts; `LEGION_ALLOC_ID` is available when they need the
ephemeral placement id.

### 6) Job Spec API

Add an optional `stateful` block to `TaskConfig` ([src/structs/job.rs:815](../../src/structs/job.rs)):

```jsonnet
{
  name: 'db',
  kind: 'container',
  count: { num: 3 },
  stateful: {
    mode: 'slot',            // fixed pool of 3 → slots 0,1,2
    storage: {
      // contract only in v1; driver TBD
      path: '/var/lib/db',
      retain_on_scale_in: true,
    },
  },
}
```

```jsonnet
{
  name: 'sandbox',
  kind: 'container',
  // count is not the controlling factor in keyed mode
  stateful: {
    mode: 'keyed',           // instances created on demand via API
    id_source: 'caller',     // 'caller' (supplied) | 'uuid' (server-minted)
    storage: { path: '/workspace' },
  },
}
```

### 7) Keyed-Mode Management API

New `Manager` service RPCs (keyed mode only):

- `CreateInstance { job_id, group, instance_id? }` → `{ instance_id }` — registers a
  new never-recycled instance and triggers placement.
- `DestroyInstance { instance_id }` → terminates the instance, detaches/GCs storage,
  and (per WI plan) stops minting tokens for its `sub`.
- `ListInstances { job_id, group }` / `GetInstance { instance_id }` — introspection.

Slot-mode instances are managed implicitly by the scheduler from `count.num`; no API
calls needed.

---

## Relationship to Workload Identity

The [WI plan](workload-identity-oidc-plan.md) issues a token whose claims are derived
server-side from the allocation. With instance identity in place:

- The WI `sub` for a stateful workload includes the instance key:
  `sub = <region>:<job>:<group>:<task>:<instance-id>`, and a `legion.io/instance`
  (plus `legion.io/slot` in slot mode) claim is emitted.
- Because the instance id is stable across allocations, the **token's identity is
  stable across reschedules** even though each allocation mints a fresh, short-lived
  token. We persist the *identity*, never the token bytes.
- Issuance reads the `instance_id` off the allocation's instance record when stamping
  claims — so the WI issuance RPC depends on the alloc→instance mapping defined here.
- Recycle safety carries straight through: keyed ids are never reused, so a dead
  sandbox's `sub` can never be re-minted for a different sandbox.

This plan must land (at least the instance primitive + alloc mapping) before the WI
plan's stateful `sub` variant can be implemented.

---

## Proto, Struct, and Persistence Changes

1. **`src/structs/`** — new `instance.rs`: `Instance`, `InstanceMode`, `InstanceState`;
   add `stateful: Option<StatefulConfig>` to `TaskConfig` and `instance_id:
   Option<String>` to `Allocation`.
2. **`proto/legion.proto`** — `CreateInstance` / `DestroyInstance` / `ListInstances` /
   `GetInstance` RPCs + messages; regenerate via `build.rs`.
3. **Migration** ([src/storage/migrations.rs](../../src/storage/migrations.rs)) —
   `instances` table; add `instance_id` column to allocations.
4. **Scheduler** ([src/manager/scheduler.rs](../../src/manager/scheduler.rs)) — slot
   assignment/reuse on placement; storage-handle reattachment; node constraints for
   node-local handles.
5. **Env injection** — emit the `LEGION_INSTANCE_*` vars in the task-launch pipeline.
6. **Docs** — new `docs/stateful-workloads.md`; cross-link from README and the WI doc.

---

## Implementation Phases

### Phase 1 — Instance primitive (slot mode)
- `Instance` struct + migration + alloc `instance_id` column.
- Scheduler assigns slots `0..count-1`, reuses slot ids on replacement allocations.
- **Exit:** a killed slot-2 allocation is replaced by a new allocation that reports the
  same `instance_id` and slot index.

### Phase 2 — Env exposure
- Inject `LEGION_INSTANCE_ID` / `LEGION_INSTANCE_SLOT` / `LEGION_ALLOC_ID` /
  `LEGION_INSTANCE_MODE` into exec + container task kinds.

### Phase 3 — Keyed mode + management API
- `CreateInstance`/`DestroyInstance`/`ListInstances`/`GetInstance`; never-recycle id
  lifecycle + GC.

### Phase 4 — State binding contract
- Instance→`storage_handle` allocation, attach-before-start / detach-on-stop hooks,
  node-affinity constraints for node-local handles. (Driver impls deferred.)

### Phase 5 — WI integration
- Surface `instance_id` to WI issuance; implement the stateful `sub` variant +
  `legion.io/instance`/`legion.io/slot` claims (in coordination with the WI plan).

---

## Testing Plan

### Unit
- Slot id derivation is deterministic and stable; keyed ids are unique and never
  reissued after destroy.
- Spec validation: `mode` required when `stateful` present; `id_source` only valid in
  keyed mode; slot mode requires a concrete `count.num`.
- Env var rendering: correct vars per mode; `LEGION_INSTANCE_SLOT` absent in keyed mode.

### Integration
- Slot reschedule: kill the allocation for slot 1 → replacement reuses slot 1's
  `instance_id` and reattaches its storage handle.
- Keyed lifecycle: `CreateInstance` → placed with stable id → killed alloc → replaced
  with same id → `DestroyInstance` → id GC'd and never reissued.
- Recycle safety: after destroying a keyed instance, creating a new one yields a
  **different** id and an empty storage handle.
- Env exposure end-to-end: a running task reads `LEGION_INSTANCE_ID` consistent across
  a restart.
- WI hand-off: token issued for a stateful allocation carries the instance-keyed `sub`
  and is stable across a reschedule (cross-checked against the WI plan's tests).

### Regression
- Stateless jobs (no `stateful` block) are unaffected; allocations carry no
  `instance_id` and behave exactly as today.

---

## Open Questions

1. **Slot id format** — `<job>-<group>-<slot>` vs an opaque id with slot as metadata.
   Affects how it reads in the WI `sub`.
2. **Scale-in retention** — keep slot storage on scale-down for later scale-up
   (K8s-style PVC retention) or reclaim it? Per-task policy?
3. **Keyed id source** — allow caller-supplied ids (collisions? validation?) or always
   server-mint UUIDs? Probably support both with collision rejection.
4. **Node-affine storage** — local-dir handles pin an instance to a node; how does that
   interact with drain/rollout? (Likely: block placement elsewhere until the handle is
   migratable, surface a clear diagnostic.)
5. **Instance vs allocation in the eval loop** — does the reconciler key on instances
   for stateful groups, and how does that compose with existing per-allocation evals?
6. **Orphan GC** — when is a keyed instance with no allocation and no recent activity
   considered abandoned?

---

## Acceptance Criteria

- A stateful (slot-mode) replica keeps a stable `instance_id`, slot index, and storage
  binding across reschedules to different nodes.
- A keyed (sandbox-mode) instance is created with a unique never-recycled id, persists
  state across allocations, and is cleanly destroyed + GC'd.
- `LEGION_INSTANCE_ID` (and `LEGION_INSTANCE_SLOT` in slot mode) are available to the
  task as environment variables at process start.
- The instance id is consumable by the WI plan to produce a reschedule-stable token
  `sub`, with keyed ids guaranteed never to be reused.
- Stateless workloads are completely unaffected.
