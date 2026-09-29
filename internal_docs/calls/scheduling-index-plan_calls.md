# Judgement Calls — Declarable Scheduling Indexes

Companion to
[`plans/scheduling-index-plan.md`](../plans/scheduling-index-plan.md).
Design-time calls where a real alternative existed, plus the calls made
during execution (marked *execution*) where reality diverged from the plan.

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
3. **(execution) The default `fit` index shipped `mode: "on"` directly** —
   the plan's ladder (shadow in Phase 2, flip in Phase 3 on soak evidence)
   collapsed into one release. What replaced the soak: the candidate-set
   equivalence property pinned in unit + integration tests, shadow-mode
   agreement asserted across constraint shapes on the live plan path, the
   empty-result scan cross-check (entry 7), and the `index_min_nodes: 128`
   arming threshold keeping small clusters off the feature entirely.
   Alternative: ship shadow-by-default and ask operators to flip — rejected
   because a default nobody flips is not a default, and the differentiator
   claim ("fast out of the box") is the point. Shadow mode still exists for
   operators introducing their own indexes.
4. **(execution) The equivalence property is asserted on candidate SETS,
   not placements.** Placement tie-breaking is deliberately random
   (`select_cached_node` shuffles equal-score nodes with `thread_rng`), so
   "index-on planning produces identical placements to scan planning" is
   unprovable even scan-vs-scan. The meaningful invariant — untruncated
   index candidates equal the scan's set, in the scan's by-id order — is
   pinned directly in a scheduler unit test and continuously by shadow
   mode, which computes exactly that comparison on the live path.
5. **Bounded orthant query, not k-NN — and ordering, not filtering.**
   "Nearest node" can be a node that's close but too small; placement
   wants *nearest among those that fit*. And the fit information only
   *orders* the shard (non-fitting nodes still return, last), so an
   unbounded query preserves the scan's candidate set exactly; only the
   explicit truncation budget narrows it. Rejected plain nearest-neighbor
   even though it's the blog post's headline.
6. **Default `fit` index includes disk, with missing-dim → zero-free
   bucket.** Alternative was cpu/memory only until workers fingerprint
   disk. The zero-free rule makes disk free today: nodes that don't
   declare `resources.disk` match disk-less requests and correctly lose
   disk-ful ones (the ledger would reject them anyway), so the dim costs
   nothing where unused and works where used.
7. **(execution) Empty index results always cross-check against the scan,
   and the refill ladder lost its middle rung.** The plan's ladder was
   truncated-k → doubled-k re-queries → scan. Shipped: shard queries are
   internally unbounded (truncation happens client-side *after*
   re-verification), an index-served empty candidate set runs one scan to
   distinguish "nothing matches" from "index under-covered" before any
   failure, and a truncated list that runs dry mid-placement widens
   straight to the full scan once. Simpler, and strictly safer than
   planned: no failure path exists where the index's opinion is final.
8. **Quantized sparse grid with static log2 bucket edges, scanned over
   occupied cells — no k-d tree, no adaptive normalization, no early
   termination in v1.** O(1) maintenance under heartbeat churn beats
   asymptotically prettier structures that degrade or rebalance; static
   edges mean no re-normalization state. Early-termination enumeration is
   recorded as a follow-up optimization, not assumed.
9. **Strict partition binding for auto-selection.** An index is usable
   only when *every* `partition_by` key is bound by an equality constraint
   (or the permit's pool — and `pool` binds *only* from the permit, since
   the partition key means the pool column, not an attribute). A looser
   rule (union across unbound partitions) finds more uses but makes
   selection unpredictable and query cost unbounded. Predictability won;
   the cost is silent fallback-to-scan, which the `job plan` `reason`
   field and shadow metrics exist to surface.
10. **(execution) Multi-task group candidate sets still scan.** The plan
    wanted the group path served "via a shared helper"; shipped v1 indexes
    only single-task candidate construction (which includes `count.per`
    tasks, unbounded). Group co-placement pins members to one node from
    combined constraint sets — wiring bindings-intersection through that
    path buys little (groups are rare and small) for real complexity.
    Recorded as a known limitation in the public doc and an open TODO.
11. **No task-level `index_hint` in v1.** Keeps the job spec, proto, and
    `maximal.jsonnet` untouched and forces the automatic selection to be
    good enough to trust. Deferred to the backlog, not rejected.
12. **(execution) The explain surface cost one proto message after all.**
    The plan claimed "Proto: none", meaning no job-spec changes — but
    per-task index usage rides `PlanJobReply`, which is wire-defined, so a
    new additive `TaskIndexUsage` message (field 5) was required.
    Backward-compatible; old clients ignore it.
13. **Settings changes require a manager restart** (like the rest of
    `schedulerSettings`) rather than live reload. Index lifecycle inside
    the read-model lock is the risky code path; not mutating it at runtime
    removes a class of bugs from v1 for a minor operator inconvenience.
14. **(execution) A budget cut inside a tied cell samples the cell
    uniformly; it does not take the cell's sorted-id prefix.** Found by
    the 2026-08-06 storm drill (aws-phase5-findings.md): 25,002 `spread`
    allocations on the first-registered quarter of a 5,000-node fleet,
    every sampled `plan_submission` candidate list drawn from that one
    slice. Cause: identically-shaped idle nodes project to ONE cell, a
    task needing 4 allocations gets a 64-candidate budget, and the
    scheduler — which asks for the whole shard best-first and truncates
    AFTER re-verification (entry 7) — cut the verified list at the budget,
    i.e. took that cell's sorted-id prefix. `sim-0-*` sorts first, so
    every plan's universe was the same 64 nodes until they hit
    `maxAllocations`, and `spread` scored among those. Fix: the query tags
    each candidate with its cell rank (`IndexCandidates::cell_ranks`),
    and `truncate_within_boundary_cell` keeps whole ranks and samples the
    rank the cut lands in (`rand::seq::index::sample`, order preserved);
    the same helper serves a budgeted `query()`. The walk across cells
    stays deterministic. Alternatives: (a) a per-query rotating
    offset into the cell — deterministic, but concurrent planners on the
    same rotation still collide, and rotation state is one more thing to
    keep coherent under the read-model lock; (b) shuffle the whole shard
    order — throws away the nearest-fit ordering that is the index's
    reason to exist; (c) raise the budget — hides the bug behind fleet
    size, and the phase-5 fleet was 78x the budget; (d) a `distinct`-style
    sub-key (hash of node id) inside cells — deterministic and fair, but
    it fixes the sample per fleet, so two planners racing for the same
    cell get identical lists, which is the contention late-binding apply
    exists to absorb rather than provoke. Cost: a truncated candidate list
    now differs between two identical `job plan` runs (documented in
    docs/scheduling-indexes.md); the untruncated set is unchanged, so the
    scan-equivalence property still holds. Ranked here because it decided
    fleet utilization and failure-domain spreading at scale — one instance
    loss took 96% of the workload on the rig.
