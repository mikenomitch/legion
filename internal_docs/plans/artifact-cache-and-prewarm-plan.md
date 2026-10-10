# Plan: Artifact Cache and Pre-Warming

> **Judgement calls:** [`calls/artifact-cache-and-prewarm-plan_calls.md`](../calls/artifact-cache-and-prewarm-plan_calls.md)

Status: **IN EXECUTION.** Phase 1 shipped in part as the node content store
(suspend plan Phase 3a): digest-pinned single-file artifacts only, per-class
budgets, copies instead of hardlinks. The deviations are recorded in
[suspend plan calls 31, 59, 92, 93, 101](../calls/suspend-resume-and-oversubscription-plan_calls.md),
and what is left of Phase 1 is in `internal_docs/TODO.md` under Artifacts. Phase 3 shipped in
part as suspend plan Phase 7a: advertisement is heartbeat deltas into an
off-raft location index (not `ReportArtifactCache` and a dynamic attribute),
and the score term is `schedulerSettings.weights.locality`, default 1.0 (not
`artifact_locality: 0.3`). See
[suspend plan calls 32, 78, 80, 89, 90, 91, 105, 106](../calls/suspend-resume-and-oversubscription-plan_calls.md#32-weightslocality-defaults-to-10-the-day-phase-7a-ships).
Suspend plan Phase 7b finished Phase 3's warming half for snapshots:
`legion snapshot warm` (the `WarmSnapshot` RPC) fetches a published
snapshot's parts onto matching nodes, as an in-memory queue on the heartbeat
rather than an `artifact_warm` eval and table (suspend plan call 82). Phase 3
is checked off in TODO.md. The Nodes-page cache card, and warming artifacts
(Phase 2), are still open.

## Problem Statement

The artifact system ([artifact-system-plan.md](artifact-system-plan.md))
makes fetching *correct*: tasks declare artifacts, workers fetch, verify,
and stage them at allocation start. It deliberately does not make fetching
*fast or frugal*:

- every allocation start fetches from origin — restarts, reschedules, and
  same-node scale-outs re-download bytes the node already had,
- two allocations landing simultaneously download the same artifact twice,
- nothing bounds how much disk artifacts consume on a node, and nothing
  ages out stale copies,
- a 50-node scale-up means 50 simultaneous cold pulls at the worst possible
  moment, and the scheduler has no idea that one node already holds what a
  task needs while another would pull gigabytes.

This plan adds the performance and capacity layer on the seams the artifact
plan left for it: a per-node **content-addressed cache** with a size
budget, TTLs, pinning, and eviction; and a **pre-warm layer** — operator-
and policy-driven pre-pulling, cache advertisement to the manager, and a
soft scheduler scoring term so warm nodes win placement on the next
scheduling pass.

`internal_docs/differentiators.md` anticipates exactly this: "Pre-fetching artifacts
in deploy process … Artifact cache generally? This should be built in for
the scaling cases. Multiple layers of pre-fetching and tagging nodes as
being pre-warmed."

## Relationship to the Artifact System Plan

This plan **requires** the artifact plan and changes none of its contracts:

- Fetchers write through `ArtifactSink`; this plan substitutes a
  content-addressed-store writer for the per-allocation temp writer.
  Fetchers, specs, and task plugins are untouched.
- Everything is already digest-identified (checksums, pinned OCI digests,
  git SHAs), which is what makes content addressing and cache advertisement
  meaningful.
- Auth is reused as-is. In particular, **node identity** (artifact plan §6)
  is the credential for pre-warm fetches — a warm pull happens before any
  task exists, so it authenticates as "a Legion worker node of this
  cluster," which that plan already defines.
- Per-artifact `artifact_states` reporting extends with cache fields
  (`cache: 'hit' | 'miss' | 'revalidated'`) on the same UI/CLI surfaces.
- The `artifacts.containerImageDesugar` cluster flag (default off in the
  artifact plan precisely because uncached image pulls would regress
  runtime-side caching) flips its recommended default to **on** once this
  plan's cache phase lands.

Dependency edges: the cache phase needs artifact plan Phase 1 (and benefits
from 4's OCI work for per-blob dedupe). Warm policies for private sources
need artifact plan Phase 2 (KV auth) or Phase 5 (node identity).
Everything in the artifact plan ships and works without this plan.

## User Scenarios

**C1 — Shared model weights on one node.** Three tasks on the same node
reference `oci://registry.acme.io/models/embedder@sha256:abc…` (4 GiB).
The node downloads it once into the cache and hardlinks it into each task
directory. A restart of any of those tasks performs zero network fetches.

**C2 — Edge node with a small disk.** A worker is configured with a 5 GiB
artifact cache. LRU eviction keeps it under budget, per-entry TTLs bound
staleness for mutable references, artifacts pinned by running allocations
are never evicted, and the cache refuses fetches that would push the node
below a configured free-disk floor.

**C3 — Scale-up without a pull storm.** The autoscaler adds 10 nodes. A
warm-on-join policy pre-pulls the hot artifact set (authenticating via node
identity) as each node registers; workers advertise their cache contents;
the next scheduling pass scores warm nodes higher, so new allocations start
in seconds instead of minutes. Pre-warm failures never block placement —
warming is only ever an optimization.

**C4 — Operator warms before a big rollout.** Before bumping `api` to v43,
an operator (or CI) runs
`legion artifact pull oci://ghcr.io/acme/api@sha256:def… --constraint pool=default`,
watches `legion artifact status` until the fleet is warm, then submits.
The rollout's allocations all cache-hit.

## Goals

- A per-worker content-addressed artifact cache: configurable size budget,
  per-entry TTLs, LRU eviction, pinning for in-use artifacts,
  single-flight dedup of concurrent fetches, and a free-disk floor.
- OCI blobs cached per-layer so images and ORAS artifacts sharing layers
  store and transfer them once.
- Pre-pull: an RPC + CLI to push artifacts to constraint-selected nodes,
  warm policies in manager config (including warm-on-join), all
  authenticated via node identity or KV.
- Cache advertisement from workers and a soft `artifact_locality` scoring
  term so the scheduler prefers warm nodes on the very next pass — without
  warming ever becoming a correctness dependency.
- Cache and warm state visible in the UI and CLI (hit/miss per artifact,
  per-node cache contents, warm progress).

## Non-Goals

- **No correctness coupling.** Placement must never *require* a warm node;
  scoring is soft. If warming never ran, staging fetches cold and
  everything works.
- **No peer-to-peer distribution.** Workers fetch from origin stores only.
  Node-to-node sharing (registry mirror on managers, torrent-style) is
  future work the cache layout leaves room for.
- **No cross-node cache coherence.** Each node's cache is independent;
  digests make coherence unnecessary.
- **No re-fetch of changed artifacts under running tasks.** TTLs govern
  *next* staging, never live allocations.

## Background: What Exists (after the artifact plan)

- Worker staging pipeline with `ArtifactSink`, digests, per-artifact
  states, retries, and on_unhealthy-routed failures.
- Node identity tokens (`sub = <region>:node:<pool>`,
  `legion.io/scope: node`) issuable over the worker's authenticated
  channel.
- Manager dispatcher that pushes RPCs to individual workers (`PostWork`
  pattern); heartbeat polling; eval queue with priority + dedup-on-claim
  (`docs/scheduler-and-eval-loop.md`).
- `UpdateNodeDynamic` merges dynamic node attrs **and enqueues `job_update`
  evals for jobs on that node** — useful precedent but the wrong trigger
  semantics for cache advertisement (see Judgement Calls).
- Scheduler scoring: constraints (hard), permits (hard), affinities (soft),
  automatic anti-affinity weighted by `schedulerSettings` — the slot where
  `artifact_locality` fits.
- `rusqlite` (bundled) for the cache index; reflink/hardlink staging is OS-
  dependent with a copy fallback.

## Proposed Design

### 1) The Content-Addressed Cache

Per-worker, with a sqlite index for crash-safe refcounts and LRU
bookkeeping:

```
<worker_state_dir>/<worker_id>/artifacts/
  index.sqlite          # entries, pins, last_use, sizes, ttl deadlines
  cas/sha256/ab/abcd…/  # immutable blobs and unpacked trees, named by digest
  tmp/                  # in-flight downloads; same fs => atomic rename in
```

Two kinds of cache key:

| Source kind | Key | TTL behavior |
| --- | --- | --- |
| Checksummed / digest-pinned (the default after `resolve: 'pin'`) | the digest itself | none — immutable; only LRU/size pressure can remove it |
| Mutable (`resolve: 'always'`, plain URLs) | hash of (canonical source, mediaType) → points at a digest | after `ttl`, next use revalidates via the fetcher's `resolve()` (cheap) or refetches; the old digest stays until evicted |

The job-spec `artifacts[]` entry gains the field the artifact plan reserved:

```jsonnet
artifacts: [
  {
    source: 'https://releases.acme.io/nightly/tool.tar.gz',
    resolve: 'always',
    // How long a cached copy of a *mutable* source may be used without
    // revalidation. Ignored for checksummed/pinned sources. Default:
    // the worker cache's defaultTtl.
    ttl: '24h',
  },
]
```

Worker configuration — a `cache` sibling to the artifact plan's
`artifacts.fetch` block:

```jsonnet
{
  workerType: 'default',
  pool: 'default',
  artifacts: {
    fetch: { parallelism: 4, attempts: 3, timeout: '10m', maxArtifactSize: '50GiB' },
    cache: {
      maxSize: '20GiB',        // budget for cas/ (C2)
      defaultTtl: '72h',       // mutable-source revalidation window
      minFreeDisk: '5GiB',     // refuse fetches that would cross this floor
      advertise: true,         // §3: report cached digests to the manager
      advertiseLimit: 256,     // cap on advertised digests
    },
  },
}
```

Store API (worker-internal; `stage_artifacts` calls `ensure` + `stage`
instead of fetching into the per-allocation temp dir):

```rust
impl ArtifactStore {
    /// Fetch-or-hit. Single-flight per cache key: concurrent allocations
    /// wanting the same artifact share one download. Returns a pin held on
    /// behalf of `owner` (an allocation, or the synthetic warm owner).
    pub fn ensure(
        &self,
        artifact: &ResolvedArtifact,
        credential: Option<Credential>,
        owner: PinOwner,
    ) -> R<PinnedArtifact>;

    /// Materialize into a task directory: reflink, else hardlink, else
    /// copy (cross-filesystem / Windows fallback). Applies mode/unpack
    /// exactly as the artifact plan's direct staging did.
    pub fn stage(&self, pin: &PinnedArtifact, task_dir: &Path, spec: &ArtifactSpec) -> R<()>;

    /// Drop all pins owned by an allocation (destroy/failure paths).
    /// Unpinned entries become eviction candidates, not deletions.
    pub fn release(&self, owner: PinOwner);

    /// Enforce budget + TTLs: LRU over unpinned entries until under
    /// maxSize and above minFreeDisk. Runs on an interval and before any
    /// fetch that would exceed budget.
    pub fn gc(&self) -> R<GcReport>;

    /// Most-recently-used digests for advertisement (§3).
    pub fn advertised_digests(&self, cap: usize) -> Vec<String>;
}

pub enum PinOwner {
    Allocation(i64),
    /// Warm pulls hold a TTL-bounded pin (default 1h) so warmed bytes
    /// survive until the rollout that wanted them — but never forever.
    Warm { expires_at: Timestamp },
}
```

Rules that make C1 and C2 hold:

- **Pinning**: every allocation that staged an artifact holds a pin until
  `destroy`. GC never touches pinned entries. If the pinned set alone
  exceeds the budget, fetches for *new* allocations fail with an explicit
  "cache budget exhausted by running allocations" error — which routes
  through normal allocation failure → reschedule elsewhere, the correct
  behavior on a genuinely full node.
- **Single-flight**: a per-key in-process lock; the second requester waits
  on the first download rather than duplicating it.
- **Atomicity**: downloads land in `tmp/` and rename into `cas/` only
  after digest verification (inherited behavior — the sink swap preserves
  it).
- **OCI per-blob storage**: image layers and ORAS blobs live in the CAS
  individually, keyed by their own digests; manifests reference them. Two
  images sharing base layers store them once (C1 at the layer grain).
- **Disk accounting**: staged copies are reflinks/hardlinks, so an
  artifact costs disk once per node regardless of how many tasks use it;
  the cache budget is the guardrail. `resources.disk` continues to
  describe the task's own writable footprint (see Judgement Calls).

### 2) Pre-Pull: RPCs, CLI, and Warm Policies

**Operator-driven pull (C4):**

```
legion artifact pull oci://ghcr.io/acme/api@sha256:abc… --constraint pool=default
legion artifact status sha256:abc…       # per-node warm progress
legion artifact ls --node <id>           # what a node's cache holds
legion artifact gc --node <id>           # force a GC pass
```

`WarmArtifacts` (manager RPC, idempotency-keyed like other mutating RPCs)
creates an `artifact_warm` eval. The eval handler selects nodes via the
existing `ConstraintSet` machinery and dispatches `PullArtifacts` to each
worker over the dispatcher's existing push channel. Workers fetch into the
cache under `PinOwner::Warm` and report progress; the manager records
per-node warm state in a small table that backs `artifact status` and the
UI.

**Warm policies (manager config):**

```jsonnet
{
  artifacts: {
    warm: [
      {
        source: 'oci://ghcr.io/acme/api@sha256:abc…',
        constraint: { pool: 'default' },
        onJoin: true,                            // also pull when a node joins
        auth: { nodeIdentity: 'artifact-pull' }, // or { kv: '…' } — never a task identity
      },
    ],
  },
}
```

`onJoin` hooks the existing `node_join` eval path — exactly the autoscaler
scale-up moment (C3).

**Auth interplay (decided by the artifact plan, consumed here).** A warm
fetch happens before any task exists, so it cannot authenticate as a task
identity — warm policies carry their own auth: `nodeIdentity` (the natural
fit: "the cluster's worker nodes are fetching") or `kv`. Admission warns
when a warm policy targets an artifact whose only job-side auth is a task
identity: warming will fetch with policy auth or not at all. Content
addressing makes credential provenance irrelevant once bytes are
digest-verified — placement-time staging cache-hits regardless of which
credential originally fetched them.

**Mutable references are second-class for warming.** Warming `:latest` is
racy (the tag may move between warm and placement). Warm requests for
un-pinned sources are accepted but flagged in CLI output; locality scoring
ignores them. The recommendation stays: pin digests for anything you warm
or roll out — which `resolve: 'pin'` already makes the default.

**Warming never degrades running work.** Warm pulls that would require
evicting *pinned* entries are refused on-node and reported in
`artifact status`, rather than squeezing running allocations.

### 3) Cache Advertisement

Workers with `advertise: true` report their cached digest set (capped at
`advertiseLimit`, MRU order) via a new `ReportArtifactCache` RPC whenever
it changes materially (debounced; piggybacking on the heartbeat cadence).
The manager stores it under `node.dynamic_attrs["legion.artifact_cache"]`.

Deliberately **not** `UpdateNodeDynamic`: that RPC auto-enqueues
`job_update` evals for every job on the node, which a 50-node warm would
turn into an eval storm for no benefit. The next scheduling pass reads node
attrs fresh anyway — which is precisely the "signal on the next scheduling
run" semantics warming wants. Constraints/affinities can also match the
attr explicitly if an operator wants hard or custom behavior.

### 4) Scheduler Scoring: `artifact_locality`

A built-in soft term alongside the existing automatic anti-affinity:

```jsonnet
// manager.jsonnet
{
  schedulerSettings: {
    artifact_locality: 0.3,   // default; 0 disables
  },
}
```

Per candidate node:
`score += artifact_locality × (task's pinned-digest artifacts present in
the node's advertised set ÷ task's total pinned-digest artifacts)`.

Only checksummed/digest-pinned artifacts participate — mutable references
have no stable identity to match, and the asymmetry deliberately rewards
the pinning that also makes rollouts deterministic. Scoring tolerates
stale/partial advertisement (it is soft and capped), which is what permits
the `advertiseLimit` bound and debouncing.

### 5) UI and Status Surfaces

Extending the artifact plan's surfaces (same components, same generated
clients):

- **Allocation artifact rows** gain `cache: hit | miss | revalidated` and
  show `staged (cache)` instantly on hits.
- **Nodes page**: per-node cache card — size used / budget, entry count,
  pinned bytes, last GC; expandable list from `artifact ls`.
- **Warm progress**: `artifact status` data rendered per-node
  (pending/fetching/warm/failed) — the operator's C4 view before a
  rollout.
- **CLI/MCP parity** per AGENTS.md: `artifact pull` is mutating
  (idempotency-keyed, `mutates: true`, gated behind `--allow-write` in
  MCP); `artifact status/ls` are read-only tools; all verbs take `--json`
  with the standard envelope; capability catalog updated.

## Proto, Struct, and Config Changes

Proto (`proto/legion.proto`, documented per repo convention):

- New manager RPCs: `WarmArtifacts` (+ idempotency key), `ArtifactWarmStatus`,
  `ReportArtifactCache`.
- New worker RPC: `PullArtifacts`.
- New eval kind: `artifact_warm`.
- `artifact_states` entries gain `cache` field.

Structs and config:

- `src/artifacts/store.rs` (the CAS + index), `PinOwner`, GC loop wiring in
  the worker daemon.
- `ArtifactSpec` += `ttl`.
- Worker config += `artifacts.cache`; manager config += `artifacts.warm`;
  `schedulerSettings.artifact_locality`.
- Manager: warm-state table, `artifact_warm` eval handler, scoring term in
  the placement policy, `legion.artifact_cache` dynamic attr convention.

## Implementation Phases

### Phase 1 — Content-addressed cache
CAS + sqlite index, sink swap, pinning + release on destroy, single-flight,
TTL/revalidation via fetcher `resolve()`, size budget + `minFreeDisk`, LRU
GC loop, `ttl` spec field, `artifact ls/gc` CLI, cache hit/miss in
`artifact_states` + UI; OCI per-blob storage; flip
`containerImageDesugar` recommended default to on.
**Win:** C1 and C2 — restarts and same-node scale-outs stop re-downloading,
concurrent allocations share one download, disk usage is bounded and
TTL-fresh, and container-image management through Legion stops costing
repeat pulls. (Depends on artifact plan Phase 1; per-blob dedupe on its
Phase 4.)

### Phase 2 — Warm RPCs and policies
`WarmArtifacts`/`PullArtifacts`, `artifact_warm` eval, warm-state table,
`artifact pull/status` CLI + MCP, warm policies + `onJoin`, policy auth via
`nodeIdentity`/`kv`, warm-vs-pinned refusal.
**Win:** C4 — operators and CI warm a fleet before a rollout and watch it
happen; autoscaled nodes start pulling the hot set the moment they join.
(Depends on Phase 1; private sources need artifact plan Phase 2 or 5.)

### Phase 3 — Advertisement + locality scoring
`ReportArtifactCache` (debounced), `legion.artifact_cache` dynamic attr,
`artifact_locality` scoring term + setting, Nodes-page cache card.
**Win:** C3 completes — the scheduler *prefers* warm nodes on the very next
pass after warming, turning pre-pull from "bytes are nearby" into "fast
placements actually happen," with zero correctness coupling. (Depends on
Phases 1–2.)

## Testing Plan

### Unit

- Cache: key derivation (pinned vs mutable), TTL expiry → revalidation
  path, LRU eviction order, pins never evicted, budget-exhausted error when
  pinned set exceeds `maxSize`, `minFreeDisk` refusal, single-flight (two
  `ensure` calls, one fetch), warm pins expire.
- Scoring: locality fraction math, weight 0 disables, mutable refs
  ignored, missing/partial advertisement tolerated.
- Warm policy validation: constraint shapes, auth one-of
  (`kv`/`nodeIdentity` only — task `identity` rejected), mutable-source
  warning surfaced.

### Integration

- Second allocation of a pinned artifact: zero network fetches (assert one
  origin hit across two allocations); hardlinked staging shares inodes
  where the platform supports it.
- ETag/digest flip on a fixture: TTL revalidation refetches; unchanged →
  reuse without download.
- Budget pressure: filling the cache evicts LRU unpinned entries; a warm
  pull that would evict pinned entries is refused and reported.
- Warm end-to-end: `WarmArtifacts` against two fake workers → both report
  via `ReportArtifactCache` → next `plan_only` run scores the warm node
  higher; **assert no `job_update` evals were enqueued by advertisement**.
- `onJoin`: a joining node receives policy pulls through the `node_join`
  eval path.
- OCI layer dedupe: two tags sharing base layers against a local registry
  fixture store shared blobs once.

### Regression

- With the cache disabled/unconfigured, the artifact plan's direct-staging
  behavior is unchanged (sink fallback).
- Allocation failure semantics unchanged: a cache-layer failure surfaces
  exactly like a fetch failure (on_unhealthy budgets).
- Eval queue behavior: `artifact_warm` evals respect priority/dedup rules;
  unknown-eval-kind handling untouched.

## Rollout & Backward Compatibility

- The cache activates only when `artifacts.cache` is configured (or via
  conservative defaults gated behind a worker flag during rollout);
  removing the config reverts to direct staging.
- `artifact_locality` defaults to a modest 0.3 and can be set to 0; it is
  soft, so worst case is status-quo placement.
- Warm policies and advertisement are opt-in config; fleets upgrade node by
  node (workers without the cache simply never advertise).
- Cache directories are self-contained under the worker state dir; wiping
  them is always safe (everything refetches).

## Judgement Calls

Split out into
[`calls/artifact-cache-and-prewarm-plan_calls.md`](../calls/artifact-cache-and-prewarm-plan_calls.md)
— seven decisions, ranked by project impact (cache as the only coordination
point, warm-pin ranking, digest-pinned-only scoring, dedicated report RPC, TTL
semantics, hardlink disk accounting, sqlite index).

## Open Questions

1. **Advertisement at scale.** A digest list capped at 256 is fine for
   realistic caches; if fleets carry thousands of entries, switch the attr
   to a bloom filter — scoring already tolerates false positives (it is a
   soft score). Decide when a real fleet hits the cap.
2. **`warmOnJoin: 'referenced'`** — auto-warming the pinned artifacts of
   *running jobs the new node could host* (beyond explicit policies) is
   the strongest scale-up story, but needs a selection bound (top-N by
   reference count? by recent placement frequency?) to avoid filling small
   caches with the long tail. Scoped out of Phase 2 until the bound is
   designed.
3. **Per-task-identity pre-pull.** Declined: no allocation → no task
   token. If demand appears, the manager could mint short-lived
   warm-scoped tokens for *declared* (not placed) tasks — security review
   required first. Node identity covers the known cases.
4. **Node-to-node distribution.** The CAS layout (digest-keyed blobs) is
   compatible with a future manager-side mirror or P2P transfer layer;
   out of scope here.
5. **Should warm completion nudge pending placements?** Today nothing
   re-evaluates a job just because a node became warm (the next natural
   eval picks it up). An optional "enqueue `job_update` for jobs with
   unplaced allocations referencing these digests" hook on warm completion
   would tighten C3 further — worth it only if real gaps show up between
   warm completion and the next eval.

## Acceptance Criteria

- **Phase 1:** second allocation of a pinned artifact performs zero network
  fetches; cache never exceeds `maxSize` with running pins honored; TTL
  expiry triggers revalidation, not blind refetch; `artifact ls/gc` work;
  UI shows cache hits; image layers shared across two tags stored once.
- **Phase 2:** `legion artifact pull` warms a constraint-selected node set
  with visible per-node progress; a joining node executes `onJoin`
  policies; warm fetches authenticate via node identity; a warm that would
  evict pinned bytes is refused and reported.
- **Phase 3:** in a two-node test, warming one node makes it win placement
  on the next pass; advertisement enqueues no `job_update` evals; setting
  `artifact_locality: 0` restores baseline placement.
