# Plan: Cheap Scheduler Plan Snapshots (Arc'd Nodes + Cached Resource Pools)

> **Judgement calls:** [`calls/scheduler-plan-snapshot-plan_calls.md`](../calls/scheduler-plan-snapshot-plan_calls.md)

**Status: SHIPPED** (single-phase perf refactor; no user-facing surface).
Scope grew once during execution, on request: the per-plan
`allocations_by_node` clone — originally deferred — is also fixed
(`Arc<Vec<Allocation>>` per node, copy-on-write; calls entry 5), along
with two smaller wrong-clones the audit surfaced (rollout deep-cloning
every node for an id set, and the epoch sample cloning every candidate
node id).

## Problem Statement

The scheduling-index work measured (`benchmarks/scheduler/README.md`) that
whole-plan wall-clock is dominated by the **per-plan snapshot**, not by
candidate selection: a 12-task plan over 5k nodes spends ~75 of ~83 ms
before any placement logic runs. Two O(all-nodes) costs repeat on every
plan:

1. `available_nodes()` → `Store::cached_nodes()` **deep-clones every
   `Node`**, including its four `serde_json::Value` attribute bags
   (`src/manager/scheduler.rs:963`, `src/storage/store.rs`).
2. `ResourceLedger::new` **re-parses every node's capacity from JSON**
   (`ResourcePool::from_node`: fingerprint memory/cpu parsing, custom
   `config_attrs.resources`, reservations) and re-normalizes every
   allocation's usage — per plan, from scratch.

The scheduling index deliberately did not touch either (its candidates are
ids; the ledger re-verifies), so single-task plans saw no wall-clock win.
This plan removes both costs.

## Prior Art

- **Nomad** keeps immutable state-store objects and hands schedulers
  copy-on-write snapshots — plans never deep-copy the node set.
- **Kubernetes** maintains a scheduler cache (`NodeInfo`) with pre-computed
  resource totals, updated by informer events, snapshotted per cycle.
- Legion already has the right seam: the `SchedulingReadModel` write path
  (8 `cache_*` functions + full reload) is exactly where both caches can be
  maintained — the scheduling indexes already hang off it and already
  recompute per-node free amounts on the same triggers, then throw them
  away.

## Goals / Non-Goals

Goals: plan-time node acquisition and ledger construction proportional to
**snapshot copy** (refcounts + small maps), not JSON size; identical
placement semantics; one atomic capacity snapshot per plan.

Non-goals: changing the epoch fencing model; touching how plugins receive
candidate nodes (`SchedulingContext` keeps `Vec<&Node>`); making `Node`
itself internally shared. (Arc'ing allocations was originally a non-goal
and was pulled into scope during execution — see the status note.)

## Proposed Design

1. **`SchedulingReadModel.nodes` becomes `HashMap<String, Arc<Node>>`.**
   The scheduler takes its per-plan node set as `Vec<Arc<Node>>`
   (`Store::available_node_arcs()` — filter + sort under one read guard,
   refcount bumps only) and immediately works with `&Node` borrows off the
   arcs, so everything downstream — constraints, permits, scoring, the
   plugin context — is untouched. In-place mutations (`cache_nodes_lost`,
   `cache_node_dynamic_attrs`) use `Arc::make_mut`. `cached_nodes()` stays
   as the deep-clone API for its two non-scheduler callers.
2. **The read model caches a free `ResourcePool` per available node**
   (`node_pools`), maintained by the same `reproject` pass that maintains
   the scheduling indexes: on any node/allocation mutation the affected
   node's pool is recomputed from scratch (`from_node` + all its
   allocations' usage) and the index re-projects **from that pool** instead
   of computing its own free amounts. One recompute, two consumers, and the
   pool can never disagree with the index.
3. **`ResourceLedger` is built from the cached pools.** Plans that free a
   job's own allocations first (Fresh mode) recompute pools *from scratch*
   only for the nodes that carried that job's allocations — exact, and
   O(affected nodes), not O(fleet). Reschedule-mode plans clone the cached
   pools as-is. Recompute-affected was chosen over "add the freed usage
   back" because usage application is not invertible for device-set
   (`Items`) resources.
4. **One atomic capacity snapshot.** `Store::plan_capacity_snapshot()`
   returns `(allocations_by_node, node_pools)` under a single read guard,
   taken *after* the epoch sample (the fence's "sample no later than the
   capacity read" rule, unchanged). Pools and allocations can therefore
   never disagree with each other, and any later mutation bumps an epoch
   the apply-time compare-and-set checks.
5. **Resource types move to `src/resources.rs`.** `ResourcePool`,
   `ResourceRequest`, `ResourceValue`, and the fingerprint/parse helpers
   become crate-internal (`pub(crate)`) residents of the resources module —
   they now have three consumers (scheduler, store, scheduling index) and
   no longer belong inside the scheduler.

## Testing

- A store-level property test: after an arbitrary mutation sequence
  (inserts, attr updates, drains, allocation apply/remove), every cached
  pool equals a from-scratch `ResourcePool` computation.
- The full existing suite must stay green unchanged — especially the
  oversubscription/epoch fencing regressions and the scheduling-index
  equivalence property (whose free amounts now come from the shared pool).
- Before/after Criterion numbers for `candidate_construction` (plan-only
  path) and targeted `scheduler_speed` cells (plan-and-apply path),
  recorded in `benchmarks/scheduler/README.md` per the AGENTS.md rule.

## Rollout

Pure internal refactor: no config, no wire change, no migration. Risk is
concentrated in "cached pool drifts from truth"; the property test plus
the single-recompute-function discipline (same one the indexes already
rely on) is the mitigation, and the ledger remains the final authority on
fit either way.
