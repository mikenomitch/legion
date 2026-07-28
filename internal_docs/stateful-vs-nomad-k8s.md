# Stateful Support: Legion vs Nomad vs Kubernetes

An honest gap analysis of Legion's stateful primitives (see
[stateful-workloads.md](../docs/stateful-workloads.md)) against the two systems users
will compare us to. Written 2026-06; updated as primitives land.

## Primitive-by-primitive

| Primitive | Kubernetes (StatefulSet) | Nomad | Legion today |
| --- | --- | --- | --- |
| Stable ordinal identity | Pod ordinal (`web-2`) | `NOMAD_ALLOC_INDEX` (`per_alloc`) | ✅ Slot mode, `{job}-{group}-{slot}` ids, `LEGION_INSTANCE_SLOT` |
| Identity survives reschedule | ✅ (pod name + PVC re-bind) | ✅ (index + `per_alloc` volume) | ✅ (instance row re-binds id + storage handle) |
| On-demand keyed instances | ❌ (no native analog; operators build it) | ⚠️ Parameterized/dispatch jobs (one-shot batch flavor) | ✅ Keyed mode: never-recycled ids, create/destroy/list API |
| Identity → credentials | Projected SA tokens per pod | Workload identity per alloc/task | ✅ OIDC tokens with instance-keyed `sub` |
| Persistent volumes | ✅ PVC templates + CSI ecosystem | ✅ Host volumes + CSI + sticky dynamic host volumes | ✅ Volume plugins: `local-dir`/`host`/`nfs`/`tmpfs` built in, CNI-style exec ABI for the rest ([docs](../docs/volumes.md)) |
| Storage retention policy | `persistentVolumeClaimRetentionPolicy` (whenScaled / whenDeleted) | Volume lifecycle is operator-managed | ✅ `retainOnScaleIn` (= whenScaled) + `retainOnStop` (= whenDeleted, governs `job stop`; `job destroy` always deletes) |
| Stable network identity | ✅ Per-pod DNS via headless Service | ⚠️ Via Consul service registration | ❌ **None** — instances are not addressable by name |
| At-most-one instance (fencing) | ✅ StatefulSet never replaces a pod on a lost node without confirmation/force-delete | ⚠️ `prevent_reschedule_on_lost` opt-in | ✅ Lease-based fencing on by default (`stateful.onLost`): worker self-fences after `selfTerminateAfter`, manager replaces only after `replaceAfter` > that — stronger than K8s's manual default; `'never'` + `instance replace` for the K8s flavor. Create-first rollouts remain rejected for stateful tasks |
| Ordered startup / scaling | ✅ OrderedReady (0..N-1, one at a time) | ❌ | ⚠️ Ordered **updates** landed (`rollout.slots.order: sequential`, one slot per step, highest first, health-gated); initial bring-up places 0..N-1 in order but is not yet health-gated per slot |
| Staged/partitioned stateful updates | ✅ `partition` rolling updates by ordinal | ⚠️ Generic update stanza (canary/max_parallel), not ordinal-aware | ✅ `rollout.slots.updateFrom` (= K8s partition): only slots ≥ N update; lowering the floor promotes the rest, ordered and health-gated |
| Storage-aware placement | ✅ Volume topology constraints | ✅ Volume-aware scheduling | ✅ Node-local volumes pin their instance; nodes without a task's plugin are filtered before scoring |
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

3. ~~**At-most-one / fencing guarantees.**~~ **Landed** (`stateful.onLost`,
   see [stateful-workloads.md](../docs/stateful-workloads.md#node-loss-and-fencing-at-most-one)):
   node-loss replacement is now gated behind a loss-confirmation window
   coordinated with a worker-side self-fence, on by default for every
   stateful task — stronger than K8s (which needs manual force-delete) and
   Nomad (opt-in `prevent_reschedule_on_lost`, no worker-side kill).
   Remaining in this area: create-first rollouts for stateful groups stay
   *rejected* rather than fenced (a deliberate same-instance overlap needs
   storage-level fencing tokens, a volume-plugin concern).

4. ~~**Ordinal-aware orchestration.**~~ **Mostly landed**
   (`rollout.slots`, see
   [stateful-workloads.md](../docs/stateful-workloads.md#ordered-rollouts-for-slot-pools)):
   one-at-a-time updates in reverse ordinal order (`order: 'sequential'`,
   the OrderedReady update analog) and partitioned rollouts ("update slots
   ≥ 2 only", `updateFrom`, the `partition` analog with the lower-the-floor
   promote workflow) are how StatefulSets de-risk quorum systems and both
   ship now. Remaining: health-gated initial bring-up — a fresh job's slots
   place in ascending order but in one eval, without per-slot gating.

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
