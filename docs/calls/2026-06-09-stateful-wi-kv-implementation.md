# Judgement Calls — Stateful Workloads, Workload Identity, Reactive KV (2026-06-09)

Decisions made while implementing the three plans in `docs/plans/` where the plans
were ambiguous, contradictory, or silent. Each entry: the call, the alternatives,
and why.

## 1. "CNI compatibility layer" in the volume plugin plan → CSI

The request asked for a volume plugin system "similar to CNI or Nomad's volume
plugins" with "a CNI compatibility layer (optional)". CNI is the *networking* plugin
standard; the storage equivalent is **CSI**. We interpreted the intent as two separate
things and did both in the plan ([volume-plugin-system-plan.md](../plans/volume-plugin-system-plan.md)):

- Adopt the **CNI-style exec-plugin model** (JSON over stdin/stdout, versioned
  contract, manifest + bin-dir discovery) as the native volume ABI.
- Provide an optional **CSI compatibility bridge** so the existing storage-driver
  ecosystem is reachable.

Alternative considered: literally bridging to CNI plugins — rejected as a category
error (CNI plugins configure network interfaces, not storage).

## 2. Volume plugins use an exec ABI, not the existing `cdylib` ABI

Legion's existing plugins (task kinds, scheduling, node checks, rollouts) are
in-process `cdylib`s. The volume plan deliberately diverges to CNI-style external
executables because volume ops are low-frequency, frequently shell out to privileged
tools, and must not crash the worker. The plan ships `local-dir` and `host` as
in-process built-ins behind the same trait so the zero-dependency path needs no
external binaries.

## 3. Stateful slot mode: assignment happens at allocation-insert, not in the scheduler plan

The plan suggested hooking slot assignment into `build_plan`. We instead bind
instances inside the store's allocation-insert transaction (`assign_slot_instance_tx`
in `src/storage/store.rs`), because plans are dry-runnable and rejectable on stale
epochs, while the insert is the single choke point every placement path goes through
(scheduler plans, `place_alloc` reconciliation, rollouts). This makes instance
creation/binding atomic with placement and required no scheduler or plan-format
changes. Consequence: the scheduler's node choice is unaffected in v1 — storage
node-affinity is deferred to the volume plugin system.

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

## 6. Legion-injected env vars win over user-declared keys

`LEGION_ALLOC_ID` / `LEGION_INSTANCE_*` overwrite same-named keys from the job spec:
they are authoritative runtime facts and a spec must not be able to forge
`LEGION_INSTANCE_ID` (mirrors Nomad's behavior for its runtime env).

## 7. Storage handle format

`local-dir/<32-char random>` via the existing `util::random_string` — the `uuid`
crate is not a dependency and an opaque token doesn't justify adding one. The handle
is contract-only (no driver mounts anything yet; see the volume plugin plan).

## 8. `LEGION_ALLOC_ID` is injected for *all* allocations

Stateless tasks get it too — it's universally useful, harmless, and matches
Nomad's `NOMAD_ALLOC_ID` precedent. The other `LEGION_INSTANCE_*` vars appear only on
stateful tasks.

## 9. Cross-version slot takeover during rollouts

Free-slot checks are scoped per job-config version while instance rows are
job-scoped, so a create-first rollout can place the new version's allocation for slot
N while the old one still runs: the instance re-points to the new allocation, and the
old allocation's deletion re-points `current_alloc_id` to the surviving allocation.
The transient double-attach window is documented as a storage-driver concern, not a
scheduler concern.

---

*(Further entries are appended below as implementation proceeds.)*
