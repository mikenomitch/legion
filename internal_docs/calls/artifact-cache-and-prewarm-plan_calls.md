# Judgement Calls — Artifact Cache and Pre-Warming

Companion to
[`plans/artifact-cache-and-prewarm-plan.md`](../plans/artifact-cache-and-prewarm-plan.md).
Decisions taken while designing the plan where a real alternative existed.
Decided here, reversible later.

1. **The cache is the only coordination point; pre-pull is a hint.** No
   "artifact placement" bookkeeping in the manager beyond advertised attrs
   and warm progress. Correctness never depends on warm state — that is
   what keeps this entire plan optional and deletable.
2. **sqlite cache index** (bundled rusqlite) over a JSON sidecar:
   crash-safe pins/refcounts and LRU queries beat hand-rolled file locking,
   and the dependency cost is zero.
3. **Dedicated `ReportArtifactCache` RPC** rather than reusing
   `UpdateNodeDynamic`, specifically to *avoid* its `job_update` eval
   fan-out on mass warming.
4. **TTL means "revalidation window for mutable sources."** Pinned
   artifacts are immutable and exempt; retention for everyone is
   LRU-under-budget. One word, one meaning.
5. **Hardlinked stages don't count against `resources.disk`.** The cache
   budget governs shared bytes; `resources.disk` governs the task's own
   writable footprint. Copies (cross-filesystem fallback) do count. Fuzzy
   at the edges, but double-counting shared layers per task would punish
   exactly the dedupe the cache exists to provide.
6. **Warm pins are TTL-bounded (default 1h) and rank below allocation
   pins**; warming refuses to evict pinned entries. Warmth should survive
   until the rollout that wanted it, and never longer at running tasks'
   expense.
7. **Only digest-pinned artifacts participate in scoring.** Mutable refs
   lack stable identity, and the asymmetry nudges users toward the pinning
   that `resolve: 'pin'` already defaults to.
