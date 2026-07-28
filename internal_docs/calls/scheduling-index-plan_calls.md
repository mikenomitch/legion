# Judgement Calls — Declarable Scheduling Indexes

Companion to
[`plans/scheduling-index-plan.md`](../plans/scheduling-index-plan.md).
Decisions taken while designing the plan where a real alternative existed.
Decided here, reversible later.

Entries are ranked by project impact, highest first.

1. **The index proposes; the ledger disposes.** Index output is only a
   candidate ordering — every candidate is re-verified by the existing
   constraint/permit/distinct/ledger chain, and the full scan remains the
   permanent fallback. The alternative (trust the index, skip re-checks)
   is faster but turns every staleness bug into a wrong placement. A slow
   placement is recoverable; a wrong one is not. This one invariant is
   what makes a *default-on* index defensible at all.
2. **One declared shape — `partition_by` × `dims` — instead of
   Tensorlake's two structures.** Reverse-index/trie and spatial index
   become the two degenerate cases of a single sharded grid. Costs a
   little generality (no per-structure tuning); buys one config surface,
   one maintenance path, and one selection algorithm.
3. **Bounded orthant query, not k-NN.** "Nearest node" can be a node
   that's close but too small; placement wants *nearest among those that
   fit* (`cell[d] >= request[d]` in every dim, then distance-ranked).
   Rejected plain nearest-neighbor even though it's the blog post's
   headline.
4. **Default `fit` index includes disk, with missing-dim → zero-free
   bucket.** Alternative was cpu/memory only until workers fingerprint
   disk. The zero-free rule makes disk free today: nodes that don't
   declare `resources.disk` match disk-less requests and correctly lose
   disk-ful ones (the ledger would reject them anyway), so the dim costs
   nothing where unused and works where used.
5. **Quantized sparse grid with static log2 bucket edges, scanned over
   occupied cells — no k-d tree, no adaptive normalization, no early
   termination in v1.** O(1) maintenance under heartbeat churn beats
   asymptotically prettier structures that degrade or rebalance; static
   edges mean no re-normalization state. Early-termination enumeration is
   recorded as a follow-up optimization, not assumed.
6. **Strict partition binding for auto-selection.** An index is usable
   only when *every* `partition_by` key is bound by an equality constraint
   (or the permit's pool). A looser rule (union across unbound
   partitions) finds more uses but makes selection unpredictable and
   query cost unbounded. Predictability won; the cost is silent
   fallback-to-scan, which the `job plan` `reason` field and shadow
   metrics exist to surface.
7. **No task-level `index_hint` in v1.** Keeps the job spec, proto, and
   `maximal.jsonnet` untouched and forces the automatic selection to be
   good enough to trust. Deferred to the Phase 4 backlog, not rejected.
8. **Settings changes require a manager restart** (like the rest of
   `schedulerSettings`) rather than live reload. Index lifecycle inside
   the read-model lock is the risky code path; not mutating it at runtime
   removes a class of bugs from v1 for a minor operator inconvenience.
