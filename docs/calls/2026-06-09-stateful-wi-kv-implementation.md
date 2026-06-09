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

## 10. Keyed mode: `count` is ignored, instances drive cardinality

A keyed task's desired allocations = its live (pending/active/rescheduling)
instances; `count.num` is ignored and `count.per` is rejected at validation. The
alternative (treating count as a floor/ceiling) couples two control loops for no
clear benefit. Submitting a keyed job places zero allocations until
`CreateInstance` is called.

## 11. Keyed storage drops on destroy — deliberately opposite of slot scale-in

Slot scale-in retains storage by default (the slot will run again and must
reattach its data). A destroyed keyed id can never run again (never-recycle), so
keeping its state attachable would be a data leak. Destroy always drops the
handle; the tombstone row is retained forever to enforce never-recycle (collision
with any row, including tombstones, is `AlreadyExists`).

## 12. `idSource` declares the default, caller-supplied ids are always honored

`idSource: 'caller'` makes `--id` required; `'uuid'`/unset lets the server mint.
A valid caller-supplied id is accepted regardless — rejecting it under
`idSource: 'uuid'` would add friction without a safety benefit (ids are validated
for charset/length/collision either way: `[A-Za-z0-9._-]{1,128}`, since ids flow
into env vars, WI subjects, and CLI args).

## 13. Allocation churn never terminates a keyed instance

Only `DestroyInstance` (or job stop/destroy) terminates keyed instances; the
allocation-release hook moves them to `rescheduling` so the reconciler replaces
them. `--group`/`--task` on create are only required when a job has more than one
keyed task — ambiguity is rejected, never guessed.

## 14. UI shows instances read-only

The job detail page surfaces stateful badges, an instances panel, and instance
pills on allocations, but offers no create/destroy buttons for keyed instances —
a hint points at `legion instance create/destroy`. UI-driven mutations (and the
authz story around them) are a separate decision; visibility was the requirement.

## 15. WI issuance authz follows the existing worker trust model (and documents the gap)

Worker↔manager RPCs today have no cryptographic worker authentication: heartbeats
carry a self-claimed node id, server TLS identity is not enforced, and join tokens
gate joining only. `IssueWorkloadIdentityToken` therefore enforces what is
enforceable in that model — the allocation must exist and be placed on the
`node_id` the caller claims (PermissionDenied otherwise) — and the gap is
documented in the proto comment plus a `TODO(worker-authn)` referencing the
join-token trust model. Inventing a one-off credential scheme just for this RPC
would have created a second auth system; binding all worker RPCs to per-node
credentials (worker mTLS) is the right fix and is out of scope.

## 16. Env-delivered tokens are mirrored to a file with an `<ENV>_FILE` pointer

Env vars cannot change after exec, so a refreshed env-delivered token needs
somewhere to land: every env identity is also written to
`.legion/identity/<alloc>/<name>/token` (mode 0400) and a companion `<ENV>_FILE`
variable points at it. `signal` handlers re-read that file; `restart` updates the
stored task env so the relaunched process gets the fresh token. This mirrors
Nomad's file-first guidance while keeping env delivery ergonomic.

## 17. OIDC endpoints via a hand-rolled tower layer, not axum

The discovery/JWKS routes are served by `OidcHttpLayer` mounted outermost on the
existing main listener (tonic 0.12 with `accept_http1(true)`), intercepting GET on
exactly two paths and passing everything else through. No axum dependency, no
coupling to tonic router internals, pure passthrough when the feature is disabled.
The issuer/jwks URLs in the discovery document come from the configured issuer URL
so they remain correct behind proxies/load balancers.

## 18. Rotation runs on the persistence Primary; keys live in SQLite

There is no raft-leader hook for background loops, but `store.persistence_role()`
(Primary/Standby/Candidate) exists — the rotation loop acts only on the Primary
and all managers serve the shared `signing_key` table, so JWKS is identical
cluster-wide. Signing keys (private PEM included) persist in the manager's SQLite,
the same trust domain as the CA material; tokens issued before a manager restart
now verify after it. Retiring keys stay published for `max(rotationAge,
defaultTtl)`; expired rows are retained for audit but never reloaded.

## 19. Failed token issuance fails the launch

If a task declares an identity and issuance fails for any reason other than the
feature being disabled, the worker records an allocation failure (so the manager
reschedules) instead of starting the task without its declared identity. Feature
disabled = identities are inert: specs validate, issuance returns
FailedPrecondition, the worker warns once.

## 20. Region claim defaults to `auto`

The manager has no region config field today; issuance stamps
`legion.io/region: "auto"` (`DEF_CLUSTER_REGION`), matching every documented `sub`
example. When a real region config lands, it threads through one constant.

## 21. No `kv_reactive_updates` feature flag

The plan called for gating reactive KV updates behind a flag, but the feature
shipped earlier without one and is default-on; gating it now would regress
existing jobs. The per-reference defaults are already opt-in (`watch` defaults to
false), which serves the flag's purpose. Likewise, the `kv_or()` optional-lookup
helper stays deferred (all references are required in v1). Both deviations are
noted in the plan's status header.

## 22. KV debounce: fixed window from first event, last event per key wins

Bursts are coalesced in a per-manager pending map flushed by a spawned task after
a fixed window from the *first* event (bounded latency, not quiet-period
extension), default `kvDebounceMs: 500`, `0` = act synchronously (the default for
embedded/test construction, so tests stay deterministic). Put-then-delete within
the window resolves through `onDelete`; the flush re-renders from current KV
state so the final config is always truthful.

## 23. `fail` surfaces as status strings, recovery flows through `onChange`

Fail marks the job `failed` and stamps its allocations
`failed:missing_kv_dependency` — free-form status strings already surfaced in
`job list`, the ledger, and the UI — rather than adding schema. Nothing is
rescheduled (no storm). Dependency edges are retained, so a later put to the
missing key recovers the job via its `onChange` action; `onChange: none` does not
auto-recover (manual `job run` does).

## 24. KV policy precedence is field-by-field

A per-entry policy overrides only the fields it sets (`watch` became
`Option<bool>` to distinguish unset from explicit false). The previous
all-or-nothing rule silently made `maximal.jsonnet`'s own example inert — which
also exposed that the example used snake_case keys serde dropped; both fixed and
locked by tests. Two other latent bugs fixed in passing: bare-before-braced `kv`
token replacement corrupting rendered templates, and file injections duplicating
on every re-render (now upserted by path).

---

*(Further entries are appended below as implementation proceeds.)*
