# Stateful Support: Legion vs Nomad vs Kubernetes

An honest gap analysis of Legion's stateful primitives (see
[stateful-workloads.md](stateful-workloads.md)) against the two systems users
will compare us to. Written 2026-06; updated as primitives land.

## Primitive-by-primitive

| Primitive | Kubernetes (StatefulSet) | Nomad | Legion today |
| --- | --- | --- | --- |
| Stable ordinal identity | Pod ordinal (`web-2`) | `NOMAD_ALLOC_INDEX` (`per_alloc`) | ✅ Slot mode, `{job}-{group}-{slot}` ids, `LEGION_INSTANCE_SLOT` |
| Identity survives reschedule | ✅ (pod name + PVC re-bind) | ✅ (index + `per_alloc` volume) | ✅ (instance row re-binds id + storage handle) |
| On-demand keyed instances | ❌ (no native analog; operators build it) | ⚠️ Parameterized/dispatch jobs (one-shot batch flavor) | ✅ Keyed mode: never-recycled ids, create/destroy/list API |
| Identity → credentials | Projected SA tokens per pod | Workload identity per alloc/task | ✅ OIDC tokens with instance-keyed `sub` |
| Persistent volumes | ✅ PVC templates + CSI ecosystem | ✅ Host volumes + CSI + sticky dynamic host volumes | ❌ **Contract only** — opaque `storage_handle`, no drivers ([plan](plans/volume-plugin-system-plan.md)) |
| Storage retention policy | `persistentVolumeClaimRetentionPolicy` (whenScaled / whenDeleted) | Volume lifecycle is operator-managed | ✅ `retainOnScaleIn` (= whenScaled) + `retainOnStop` (= whenDeleted, governs `job stop`; `job destroy` always deletes) |
| Stable network identity | ✅ Per-pod DNS via headless Service | ⚠️ Via Consul service registration | ❌ **None** — instances are not addressable by name |
| At-most-one instance (fencing) | ✅ StatefulSet never replaces a pod on a lost node without confirmation/force-delete | ⚠️ `prevent_reschedule_on_lost` opt-in | ❌ **No guarantee** — node-loss replacement and create-first rollouts allow transient double-attach |
| Ordered startup / scaling | ✅ OrderedReady (0..N-1, one at a time) | ❌ | ❌ Slots place in parallel |
| Staged/partitioned stateful updates | ✅ `partition` rolling updates by ordinal | ⚠️ Generic update stanza (canary/max_parallel), not ordinal-aware | ⚠️ Generic rollout strategies, not slot-aware |
| Storage-aware placement | ✅ Volume topology constraints | ✅ Volume-aware scheduling | ❌ Deferred to volume plugins (node-local pinning) |
| Data migration on drain | ❌ (storage-class dependent) | ⚠️ `migrate` for sticky ephemeral disk | ❌ |
| Per-instance ops (stop/start one) | ⚠️ `kubectl delete pod` (recreated); no pause | ❌ (alloc stop = reschedule) | ✅ keyed stop/start (in progress); ❌ per-slot |

## What's genuinely missing (ranked)

1. **Volumes.** The largest gap and a prerequisite for calling stateful support
   real: today the storage handle is bookkeeping, no bytes move. The
   [volume plugin system plan](plans/volume-plugin-system-plan.md) covers it
   (exec ABI, `local-dir`/`host` built-ins, optional CSI bridge). Both
   competitors lean on the CSI ecosystem here; the bridge is what closes the
   long tail.

2. **Stable network identity.** A StatefulSet replica is *addressable*
   (`db-2.db.svc…`); peers can find it across reschedules. Legion instances
   have stable ids but no name → address mapping — clustered databases can't
   discover each other without an external registry. Needs a service-discovery
   primitive (DNS or registry API) keyed on `instance_id`. This is the biggest
   non-storage gap.

3. **At-most-one / fencing guarantees.** Single-writer systems (most databases)
   need certainty that slot N's old allocation is dead before a replacement
   binds slot N's storage. Today a lost node's allocation is replaced
   optimistically, and create-first rollouts deliberately overlap old+new on
   the same instance. Needed: a per-task `replace_on_lost: never | fenced |
   eager` knob and a stop-then-start update mode for stateful groups, so the
   safe behavior is *declarable* (K8s makes it the default; Nomad gates it
   behind `prevent_reschedule_on_lost`).

4. **Ordinal-aware orchestration.** Ordered bring-up (slot 0 before slot 1),
   one-at-a-time updates in reverse ordinal order, and partitioned rollouts
   ("update slots ≥ 2 only") are how StatefulSets de-risk quorum systems.
   Legion's rollout strategies are group-generic and should grow a slot-aware
   strategy plugin.

5. **Storage requests in the spec.** `storage: { path }` names a mount point
   but not a size, access mode, or class. The volume plan reserves `params`;
   first-class `size`/`access_mode` fields (validated, capacity-aware
   placement later) keep specs portable across plugins.

6. **Per-slot operations.** Keyed instances are getting stop/start; slots have
   no equivalent ("restart replica 2", "pin replica 1 off this node"). Less
   urgent — `count` changes and job updates cover most cases — but operators
   coming from `kubectl delete pod db-2` will look for it.

7. **Scale-down protocol.** Neither competitor really solves graceful
   decommission (drain replica, transfer leadership, then remove) — K8s added
   only retention policy, Nomad nothing. Worth a hook eventually
   (`on_scale_in: <task action>`), not a v1 blocker.

## What Legion already does *better*

- **Keyed mode** has no clean analog: K8s StatefulSets can't do sparse,
  on-demand, never-recycled identities; Nomad dispatch jobs are batch-shaped.
  Sandbox/per-tenant workloads are first-class here.
- **Identity tokens are instance-keyed out of the box** — Nomad's workload
  identity is alloc/task-scoped; a rescheduled replica keeps its `sub` in
  Legion without extra config.
- **Never-recycle as a mode property** (tombstoned ids) is a security
  guarantee neither system states explicitly.
