# Judgement Calls — Cheap Scheduler Plan Snapshots

Companion to
[`plans/scheduler-plan-snapshot-plan.md`](../plans/scheduler-plan-snapshot-plan.md).
Decisions taken while removing the per-plan node-clone and capacity-reparse
costs, where a real alternative existed.

Entries are ranked by project impact, highest first.

1. **Recompute-affected-nodes, not add-back.** A Fresh plan frees the job's
   own allocations before planning; the ledger needs pools *as if* those
   allocations were gone. Adding their usage back onto the cached pool is
   cheaper but not invertible for device-set (`Items`) resources — an item
   the node no longer declares would be "restored" into existence. Instead
   the ledger recomputes from scratch exactly the nodes the plan's filter
   touched (O(replicas), not O(fleet)), which is bit-identical to a full
   rebuild by construction.
2. **Capacity parsing became total (never errors), and that widens
   availability.** `ResourceLedger::new` used to propagate any node's or
   allocation's malformed resources as a plan error — one bad row failed
   *every* plan in the cluster. The cached recompute path runs on
   heartbeats, where erroring is not an option, so `free_for_node` instead
   projects a poisoned node as an **empty pool** (fits nothing, visible as
   zero candidates) and warns with the node id. Deliberate trade: a corrupt
   row now degrades one node instead of halting all placement, at the cost
   of a quieter failure than the old plan-fatal error. The "assuming
   infinite capacity" per-plan warning for unfingerprinted nodes went with
   it (the INFINITY encoding and its behavior are unchanged and
   documented).
3. **`Arc<Node>` at the read-model boundary, not `Arc<Value>` fields inside
   `Node`.** Arc'ing the four JSON bags inside `Node` would make every
   clone cheap everywhere, but it changes a struct used across the whole
   codebase (including serde and the worker). Arc'ing whole nodes confines
   the change to the store and the scheduler's snapshot acquisition —
   everything below candidate construction still sees plain `&Node`
   borrows, and the plugin ABI (`SchedulingContext`) is untouched.
   `Arc::make_mut` gives copy-on-write for the in-place mutations
   (heartbeat status, dynamic attrs) while older plan snapshots stay
   consistent.
4. **One recompute, two consumers.** The scheduling index previously
   computed its own free amounts per reprojection and threw them away; the
   pool cache and the index now both derive from a single
   `ResourcePool::free_for_node` call in the same write-locked pass, so
   they cannot disagree with each other — and a store-level property test
   pins cached == from-scratch across the whole mutation surface.
5. **(execution) Allocations went `Arc<Vec<Allocation>>`-per-node after
   all** — originally deferred (calls history: "ripples through far more
   signatures for a smaller win"), pulled into scope on request during
   execution. The shape that made it tractable: Arc the *vector*, not each
   allocation, so a plan's snapshot is refcount bumps, the plan's own
   Fresh-mode filter copy-on-writes only the planned job's nodes (guarded
   by a read-only `any()` so untouched nodes never copy), and the three
   read-model writers go through `Arc::make_mut`. Scoring reads are
   auto-deref-transparent; the visible cost was retyping the map
   (`SharedNodeAllocations`) through the plan path and the scheduler-plugin
   `SchedulingContext.allocations` field — an accepted plugin-facing type
   change (plugins build in-tree against the same crate).
6. **Pools and allocations snapshot under one guard; the node list stays a
   separate read.** Splitting nodes from capacity preserves the existing
   epoch-fence ordering (sample versions after the node list, before the
   capacity read) without holding any lock across planning. A node that
   drains or disappears between the two reads now simply has no pool and
   fits nothing — strictly safer than the old behavior, which would happily
   plan onto the stale node.
7. **Pools exist only for available nodes.** Mirrors index membership and
   what plans can use. A capacity view for cordoned nodes (UI, operator
   tooling) would be a separate, read-path feature.
8. **(execution) Two smaller wrong-clones from the same audit.** Rollout
   node gating deep-cloned every `Node` to build an id set — now
   `Store::available_node_ids()` (id strings only, one lock pass). The
   epoch fence cloned every candidate node id into a `Vec<String>` per
   plan — `allocation_version_vector_for_candidates` now borrows
   `&[&str]`. Both are legibility-neutral; neither warranted its own
   measurement.
