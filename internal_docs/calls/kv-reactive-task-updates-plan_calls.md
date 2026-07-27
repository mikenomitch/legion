# Judgement Calls — KV-Driven Task Updates, Injection, and Capabilities

Companion to
[`plans/kv-reactive-task-updates-plan.md`](../plans/kv-reactive-task-updates-plan.md).
Decisions made while implementing the plan where it was ambiguous,
contradictory, or silent. Recorded during the 2026-06-09 implementation pass.

## 1. No `kv_reactive_updates` feature flag

The plan called for gating reactive KV updates behind a flag, but the feature
shipped earlier without one and is default-on; gating it now would regress
existing jobs. The per-reference defaults are already opt-in (`watch` defaults to
false), which serves the flag's purpose. Likewise, the `kv_or()` optional-lookup
helper stays deferred (all references are required in v1). Both deviations are
noted in the plan's status header.

## 2. Debounce: fixed window from first event, last event per key wins

Bursts are coalesced in a per-manager pending map flushed by a spawned task after
a fixed window from the *first* event (bounded latency, not quiet-period
extension), default `kvDebounceMs: 500`, `0` = act synchronously (the default for
embedded/test construction, so tests stay deterministic). Put-then-delete within
the window resolves through `onDelete`; the flush re-renders from current KV
state so the final config is always truthful.

## 3. `fail` surfaces as status strings, recovery flows through `onChange`

Fail marks the job `failed` and stamps its allocations
`failed:missing_kv_dependency` — free-form status strings already surfaced in
`job list`, the ledger, and the UI — rather than adding schema. Nothing is
rescheduled (no storm). Dependency edges are retained, so a later put to the
missing key recovers the job via its `onChange` action; `onChange: none` does not
auto-recover (manual `job run` does).

## 4. Policy precedence is field-by-field

A per-entry policy overrides only the fields it sets (`watch` became
`Option<bool>` to distinguish unset from explicit false). The previous
all-or-nothing rule silently made `maximal.jsonnet`'s own example inert — which
also exposed that the example used snake_case keys serde dropped; both fixed and
locked by tests. Two other latent bugs fixed in passing: bare-before-braced `kv`
token replacement corrupting rendered templates, and file injections duplicating
on every re-render (now upserted by path).

## 5. Red-team fixes (2026-06 adversarial review)

Reactive KV reconciliation now skips stopped jobs, and `on_delete=fail` scopes
the failed stamp to the impacted task and recovers (including via manual
`job run` under `on_change=none`).
