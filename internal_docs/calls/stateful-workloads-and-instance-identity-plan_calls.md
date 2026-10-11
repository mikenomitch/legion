# Judgement Calls — Stateful Workloads & Instance Identity

Companion to
[`plans/stateful-workloads-and-instance-identity-plan.md`](../plans/stateful-workloads-and-instance-identity-plan.md).
Decisions made while implementing the plan where it was ambiguous,
contradictory, or silent. Each entry: the call, the alternatives, and why.
Recorded during the 2026-06-09 implementation pass.

Entries are ranked by project impact, highest first.

## 1. Fencing is two cooperating timers, not a new state machine

`stateful.onLost.selfTerminateAfter` (default 90s) has the worker kill stateful
tasks after losing manager contact; `replaceAfter` (default 2×, `'never'`
supported) has the manager hold a lost node's stateful allocations until the
loss is *confirmed* — by which point the worker has provably self-fenced or is
truly dead. Validation enforces `replaceAfter > selfTerminateAfter`. Gated
allocations keep their rows and the instance stays bound (no new lifecycle
state); the diagnostic is a computed `pending-loss-confirmation` detail, and
`legion instance replace` is the operator force path (K8s force-delete analog).
Known bounded hole, documented: config updates still replace allocations on
partitioned nodes — overlap is bounded by `selfTerminateAfter`. Drive-by fix:
the worker heartbeat loop used to die on a single failed heartbeat (`?`); it
now retries with backoff and redrives failure reports.

## 2. Slot mode: assignment happens at allocation-insert, not in the scheduler plan

The plan suggested hooking slot assignment into `build_plan`. We instead bind
instances inside the store's allocation-insert transaction (`assign_slot_instance_tx`
in `src/storage/store.rs`), because plans are dry-runnable and rejectable on stale
epochs, while the insert is the single choke point every placement path goes through
(scheduler plans, `place_alloc` reconciliation, rollouts). This makes instance
creation/binding atomic with placement and required no scheduler or plan-format
changes. Consequence: the scheduler's node choice is unaffected in v1 — storage
node-affinity is deferred to the volume plugin system.

## 3. Keyed mode: `count` is ignored, instances drive cardinality

A keyed task's desired allocations = its live (pending/active/rescheduling)
instances; `count.num` is ignored and `count.per` is rejected at validation. The
alternative (treating count as a floor/ceiling) couples two control loops for no
clear benefit. Submitting a keyed job places zero allocations until
`CreateInstance` is called.

## 4. Slot id format and uniqueness rules

`instance_id = "{job_id}-{group}-{slot}"` (deterministic, human-legible, feeds the WI
`sub`). Because ids derive from job+group+slot, validation enforces **at most one
slot-mode stateful task per group** and rejects `count.per` (per-attribute pools
would collide on slot ids).

## 5. Scale-in retention defaults to *retain*

`retainOnScaleIn` defaults to `true` (K8s-PVC-style): scaling down never implicitly
deletes a replica's storage handle; scaling back up reattaches it. Opting into
reclamation is explicit. `job stop` terminates instances but retains rows + handles;
`job destroy` cascades the rows away.

## 6. Keyed storage drops on destroy — deliberately opposite of slot scale-in

Slot scale-in retains storage by default (the slot will run again and must
reattach its data). A destroyed keyed id can never run again (never-recycle), so
keeping its state attachable would be a data leak. Destroy always drops the
handle; the tombstone row is retained forever to enforce never-recycle (collision
with any row, including tombstones, is `AlreadyExists`).

## 7. Ordered rollouts: `order: 'sequential'` + `updateFrom`, not K8s names

OrderedReady → `rollout.slots.order: 'sequential'` (one slot at a time,
highest-first like StatefulSet updates, health-gated); partition →
`updateFrom: N` (only slots ≥ N update; lowering N on resubmit promotes the
rest, ordered and gated). `updateFrom` requires `sequential` — a partition is
incoherent without ordinal replacement — and `updateFrom >= count` is rejected
("would update nothing"). Fresh-job bring-up ordering (place slot 0 before 1)
is deferred and documented honestly; updates are the safety-critical half.

## 8. K8s retention parity maps `whenDeleted` to job *stop*

`retainOnStop` (default true) governs slot storage handles at `job stop`;
`job destroy` stays an unconditional purge. Legion's destroy removes the job's
entire record history, so stop is the honest analog of "deleting the
StatefulSet". Keyed semantics stay fixed per instance and the knob is rejected
there. Handles drop only for rows the stop itself transitions — scale-in
leftovers keep theirs, matching the K8s caveat that `whenDeleted` doesn't touch
PVCs from prior scale-downs.

## 9. Red-team fixes (see docs review findings, 2026-06)

Two adversarial reviews ran against the committed branch. Fixed in this plan's
area:

- **P0** — slot placement could adopt another job's instance row sharing a
  derived slot id (cross-job storage/identity leak). Slot binding now verifies
  job/group/slot/mode; keyed ids can't alias a same-job slot pattern; the global
  id-collision check backstops cross-job cases.
- **P1** — create-first rollouts are rejected for stateful tasks at admission,
  preserving at-most-one-live-allocation until fencing exists (see the
  [gap analysis](../stateful-vs-nomad-k8s.md)).

The KV and workload-identity halves of the same review are recorded in
[`kv-reactive-task-updates-plan_calls.md`](kv-reactive-task-updates-plan_calls.md)
and
[`workload-identity-oidc-plan_calls.md`](workload-identity-oidc-plan_calls.md).

## 10. Keyed instance stop/start (pause without tombstoning)

Added `InstanceState::Stopped`: `instance stop` pauses an instance (allocation
stopped, row + storage handle retained, id still owned) and `instance start`
revives it — distinct from `destroy`, which tombstones the id forever. Job stop
now *stops* keyed instances instead of terminating them, so resubmit + start
revives them with their data; only destroy/job-destroy is permanent. Seed lists
(`stateful.initialInstances`) and batch create (`CreateInstances`, `instance
create --id a --id b --count N`) let a keyed job spin up many instances at once;
seed ids are a one-time seed, not a reconciled desired set.

## 11. Cross-version slot takeover during rollouts

Free-slot checks are scoped per job-config version while instance rows are
job-scoped, so a create-first rollout can place the new version's allocation for slot
N while the old one still runs: the instance re-points to the new allocation, and the
old allocation's deletion re-points `current_alloc_id` to the surviving allocation.
The transient double-attach window is documented as a storage-driver concern, not a
scheduler concern.

## 12. `idSource` declares the default, caller-supplied ids are always honored

> **Revised (suspend-resume PR #15 review).** `idSource` became
> `requireId = true`. The two-value enum only ever decided whether a create
> without an id is rejected, and `"uuid"` read as if it forced generated ids
> when a caller's id still won. The behavior is unchanged. No alias for the
> old name.

`idSource: 'caller'` makes `--id` required; `'uuid'`/unset lets the server mint.
A valid caller-supplied id is accepted regardless — rejecting it under
`idSource: 'uuid'` would add friction without a safety benefit (ids are validated
for charset/length/collision either way: `[A-Za-z0-9._-]{1,128}`, since ids flow
into env vars, WI subjects, and CLI args).

## 13. Keyed tasks reject an explicit `count` at parse time

Rather than silently ignoring `count` on a keyed task (the original "count is
ignored" behavior), the spec parser now distinguishes an explicit `count` from
the default and rejects it with an actionable error. Cardinality is owned by the
instance management API; a meaningless knob in the spec is a footgun, so we fail
loud. Slot tasks still require `count`.

## 14. Allocation churn never terminates a keyed instance

Only `DestroyInstance` (or job stop/destroy) terminates keyed instances; the
allocation-release hook moves them to `rescheduling` so the reconciler replaces
them. `--group`/`--task` on create are only required when a job has more than one
keyed task — ambiguity is rejected, never guessed.

## 15. Legion-injected env vars win over user-declared keys

`LEGION_ALLOC_ID` / `LEGION_INSTANCE_*` overwrite same-named keys from the job spec:
they are authoritative runtime facts and a spec must not be able to forge
`LEGION_INSTANCE_ID` (mirrors Nomad's behavior for its runtime env).

## 16. `LEGION_ALLOC_ID` is injected for *all* allocations

Stateless tasks get it too — it's universally useful, harmless, and matches
Nomad's `NOMAD_ALLOC_ID` precedent. The other `LEGION_INSTANCE_*` vars appear only on
stateful tasks.

## 17. Storage handle format

`local-dir/<32-char random>` via the existing `util::random_string` — the `uuid`
crate is not a dependency and an opaque token doesn't justify adding one. The handle
is contract-only (no driver mounts anything yet; see the
[volume plugin plan](../plans/volume-plugin-system-plan.md)).

## 18. UI shows instances read-only

The job detail page surfaces stateful badges, an instances panel, and instance
pills on allocations, but offers no create/destroy buttons for keyed instances —
a hint points at `legion instance create/destroy`. UI-driven mutations (and the
authz story around them) are a separate decision; visibility was the requirement.

## Comparison to Nomad/K8s

[`internal_docs/stateful-vs-nomad-k8s.md`](../stateful-vs-nomad-k8s.md) records the
honest gaps in the basic primitives: volumes (handle-only today), stable network
identity (no name→address mapping), at-most-one/fencing guarantees, and
ordinal-aware orchestration are the four that matter; keyed mode and
instance-keyed OIDC subjects are where Legion leads.
