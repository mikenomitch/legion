# Plan: Late-Binding Plan Apply (Candidate Lists with Commit-Time Node Choice)

> **Judgement calls:** [`calls/late-binding-plan-apply-plan_calls.md`](../calls/late-binding-plan-apply-plan_calls.md)

**Status: PLANNED.** Nothing here is built. Deliberately deferred to the
backlog phase (§8, Phase 4): capacity-aware candidate skipping,
per-component partial commit, striped disjoint candidate lists for
identical replicas, and any serialized/batched lane for physically-full
pools.

## Problem Statement

Legion applies placement plans with Omega-style optimistic concurrency:
the planner works against a snapshot, submits exactly one node per
placement together with a version vector (the job-config epoch plus a
per-node epoch for each placed-on node, `src/manager/scheduler.rs:810`),
and the apply path rejects the whole plan if any epoch moved
(`src/storage/store.rs:2187`). Conflicts are already scoped as tightly as
optimistic concurrency allows — two plans conflict only when they touch
the same node.

The problem is that concurrent planners *reliably* touch the same node:

- Eval workers default to one per CPU core
  (`default_eval_threads`, `src/structs/config.rs:1355`), so a burst of
  independent job submits genuinely plans in parallel.
- In Fresh mode (the submit path), `bin_pack`/`spread` pick the
  argmax-scoring node, with random tie-breaking only among *exact* score
  ties (within `1e-9`, `src/manager/scheduler.rs:2294`). The 1% score
  jitter band exists but arms only in Reschedule mode
  (`src/manager/scheduler.rs:1016`). Concurrent planners read the same
  snapshot, compute the same scores, and pick the same node.
- One wins; the rest hit the CAS, sleep 25–500 ms, and replan — and
  converge on the same node again. A burst of N single-allocation jobs
  degenerates to ~N serial apply rounds with O(N²) wasted planning.
- The retry budget defaults to **3**
  (`plan_conflict_max_retries`, `src/structs/eval.rs:355`), and an eval
  that exhausts it is marked failed — and **failed evals are never
  retried** (`src/storage/store.rs:9525`, the comment on
  `release_eval_for_retry`). The job silently never places.

What a user has to do today: raise `plan_conflict_max_retries` toward its
cap of 20, set `nodes_considered` to randomly decorrelate planners at the
cost of placement quality, split pools by hand — and still accept that a
large enough burst drops work.

This matters most for the workload Legion is actively courting: sandbox
platforms spawning many independent single-task jobs at once, and
contended GPU pools where several tenants' submits race for the same few
nodes. The declarable scheduling indexes
([scheduling-index-plan.md](scheduling-index-plan.md)) sharpened the
issue without changing its semantics: planning is now fast enough that
more planners reach the apply CAS per second, and the index's
deterministic tight ordering makes their candidate sets even more
identical than the scan's were.

## Prior Art

- **Omega** (Schwarzkopf et al., EuroSys 2013) — the shared-state
  optimistic model Legion already implements, including the observation
  that conflict rate, not planning cost, bounds throughput under
  contention. Omega's answer is **incremental transactions**: commit the
  placements that don't conflict instead of rejecting the whole
  transaction.
  - **Take:** conflict-scoped commits; the framing that optimistic
    concurrency is the right default and contention is what needs a
    dedicated mechanism.
  - **Reject (for v1):** full incremental commit of arbitrary
    transactions — partial commit changes plan semantics and is deferred
    to the backlog (§8 Phase 4) until component-scoped application (§6.4)
    proves insufficient.
- **Sparrow** (Ousterhout et al., SOSP 2013) — **late binding**: a task
  is not bound to the worker chosen at schedule time; it binds to
  whichever probed worker frees up first. Also the "power of k choices"
  result: sampling k candidates instead of one collapses tail latency
  under contention.
  - **Take:** defer the final node choice to commit time; k candidates
    per placement; randomization to decorrelate competitors.
  - **Reject:** Sparrow's worker-side queues and probe RPCs. Legion's
    binding point is the store's apply transaction, so late binding here
    is purely a storage-layer walk — no new distributed machinery.
- **Kubernetes** — one scheduler binds serially through the apiserver;
  racing schedulers surface as binding conflicts that requeue the pod.
  Nothing like candidate fallback exists; the scale answer is "don't run
  competing schedulers against one pool."
- **Nomad** — one scheduler process per region evaluates plans and a
  central plan applier rejects conflicting node updates; rejected
  placements are replanned. Same shape as Legion today (plan/apply split
  with a serialized applier), same all-or-nothing rejection granularity
  per node.

Neither incumbent lets a plan carry alternatives. That is the
differentiator this plan adds: **the plan proposes a ranked list, the
applier binds to the first still-valid candidate, and a race costs a
rank instead of a retry.**

## Jobs To Be Done

**Job author / platform builder (sandbox service)**
- When I spawn hundreds of sandboxes at once and the cluster has room, I
  want every one of them to start promptly, so my users are not waiting
  behind a queue that is mostly the scheduler arguing with itself.
- When two of my spawns race, I want the loser to quietly take the
  next-best machine, so racing is invisible to me instead of a failure I
  have to retry.

**Cluster operator**
- When many teams' submits target my contended GPU pool, I want placement
  throughput to track actual capacity rather than retry luck, so I size
  the pool by demand and not by scheduler overhead.
- When the pool genuinely has no room, I want that to look like "no
  capacity," not like a conflict storm, so I know whether to add nodes or
  tune the scheduler.

**On-call responder**
- When placements are slow or missing, I want to see whether plans are
  conflicting and which fallback ranks are landing, so I can tell
  contention from capacity exhaustion at a glance.

**Autonomous agent**
- When I submit a job and the cluster has capacity, I want the submit to
  eventually place without my own polling-and-resubmit loop, so one
  `job run` is one durable intent. (Today a conflict-exhausted eval
  silently drops the work — the agent has no signal to retry on.)

### User scenarios

1. **Sandbox burst.** A platform fires 200 single-task job submits in one
   second at a 500-node pool with headroom. Every eval places on its
   first apply; a handful land on their 2nd–4th candidate. Zero conflict
   retries, zero failed evals. `legion events list` shows
   `plan.applied` events with `candidateRank > 0` on the raced ones.
2. **Contended GPU pool, nearly full.** Eight submits race for a pool
   where only three nodes fit. Three plans bind (two on fallback ranks);
   five exhaust their candidates, requeue with backoff instead of
   failing, and place as capacity frees — or keep waiting, visibly
   pending, never dropped.
3. **Nothing configured.** An operator upgrades and touches no settings.
   Behavior under no contention is byte-for-byte today's: same argmax
   placements, same events, same `job plan` output plus a new
   candidates field.

## Goals

- A burst of N independent single-allocation jobs on a pool with
  headroom places with ~zero conflict rejections and zero dropped evals.
- **No work is silently dropped**: a plan that keeps conflicting requeues
  with backoff; "failed forever after three races" ceases to exist.
- Uncontended placement quality is unchanged — the first candidate is
  today's argmax pick.
- The correctness invariant is untouched: no plan can oversubscribe a
  node, with or without candidates
  (`concurrent_plans_cannot_oversubscribe_a_node` and the epoch-fencing
  suites stay green unmodified).
- Apply stays deterministic given committed state + the submission, so
  raft replay is unaffected.

## Non-Goals

- **No capacity math in the applier.** "Free" is defined by the epoch
  (§6.1); the applier never reconstructs a `ResourceLedger`. A candidate
  that was touched-but-still-fits is skipped conservatively.
- **No fix for physically-full pools.** When only one node fits,
  candidate lists have length 1 and behavior degenerates to today's
  (plus the requeue backstop). Batching or a serialized per-pool lane is
  a separate, metrics-gated decision (§11).
- **No partial commit in v1–v3.** A coupled component (§6.4) still
  applies all-or-nothing; per-component partial commit is backlog.
- **No job-spec surface.** The knobs are `schedulerSettings` only; job
  authors see nothing new.
- **Not the apply-time capacity-shrink fix.** Re-validating node resource
  *totals* at apply (worker re-registration shrinking capacity) is an
  existing, separate TODO item; this plan neither fixes nor worsens it.

## Background — What Exists Today

- **Plan submission.** `create_plan_submission`
  (`src/storage/store.rs:2095`) persists
  `placements: Vec<(node_id, task_id)>` as JSON plus
  `expected_version_vector`. The scheduler samples epochs for **every
  candidate node** before planning
  (`allocation_version_vector_for_candidates`,
  `src/storage/store.rs:1933`, called at
  `src/manager/scheduler.rs:715`) and narrows to the placed-on subset at
  submit (`expected_versions_for`, `src/manager/scheduler.rs:878`). The
  epochs come from an in-memory mirror kept coherent with the allocation
  cache (the 2026-07-28 plan-fence fix).
- **Apply.** `apply_plan_submission_apply` (`src/storage/store.rs:2176`)
  is a replicated scheduler-lane op: compare the full vector (`:2187`,
  all-or-nothing), re-validate availability/constraints/permit on the
  fresh node row per placement (`:2282`), assign networks — static port
  collisions fail placement with a node-scoped error
  (`src/storage/store.rs:6512`) — insert allocations, bump the touched
  node and job-config epochs (`:2352`).
- **Conflict retry.** `Eval::plan_with_conflict_retries`
  (`src/structs/eval.rs:293`): full replan per attempt, adaptive backoff
  (base 25 ms, cap 500 ms, jitter), default 3 attempts, then the eval is
  failed terminally (`complete_eval_impl`, `src/storage/store.rs:9558`).
  `release_eval_for_retry` (`src/storage/store.rs:9540`) already exists
  — built for foreign-lane `NotRaftLeader` rejections — and requeues a
  claimed eval as `pending` with a delay.
- **Selection.** `select_cached_node` (`src/manager/scheduler.rs:2279`)
  scores eligible nodes and returns the argmax with exact-tie shuffle;
  the jitter variant (`:2308`, band
  `RESCHEDULE_SCORE_JITTER_THRESHOLD = 0.01`, `:28`) is Reschedule-only.
  The scored list the selector already computes is exactly the ranking a
  candidate list needs — today all but the winner is discarded.
- **Coupling machinery that already exists:** `DistinctTracker`
  (`src/constraints.rs`) threads distinct-constraint state across
  placements in one plan; group co-placement builds combined candidate
  sets in `build_plan`; colocation affinities
  (`task_colocation_preferences`, `src/manager/scheduler.rs:2729`) softly
  couple tasks; Reschedule mode already knows how to "place only what is
  missing" with excluded nodes — the refill shape Phase 4's partial
  commit would reuse.

## Proposed Design

### 6.1 The core semantic: "free" means "epoch unchanged", and the applier walks

A plan submission carries, per placement, an **ordered candidate list**
instead of a single node. Each candidate's node epoch is in the
submission's version vector (the scheduler already samples all of them —
the narrowing step at `scheduler.rs:810` widens from "placed-on nodes" to
"union of candidate lists" and otherwise disappears).

The apply transaction walks each placement's list and binds to the
**first candidate whose node epoch still matches the sampled value**:

- Epoch unchanged ⇒ no allocation was added or removed on that node
  since the planner verified fit there ⇒ the planner's ledger math is
  still valid. No capacity computation happens in the applier — the
  epoch *is* the fit proof, carried forward from plan time.
- Epoch moved ⇒ skip the candidate. Conservative by design: a node that
  was touched but still fits is passed over, costing at most a
  lower-ranked (near-equivalent, §6.2) placement — never correctness.
- The existing per-placement fresh-row re-validation
  (availability, constraints, permit — `store.rs:2282`) runs on the
  candidate being attempted; a failure there **skips to the next
  candidate** instead of rejecting the plan. This also absorbs the
  cordon-between-snapshot-and-apply race as a fallback instead of a
  rejection.
- A static-port collision on a candidate likewise skips to the next
  candidate rather than failing the placement — same conservative rule,
  one more reject class converted to a fallback.
- All candidates stale ⇒ the placement (and, in v1, the plan) rejects
  with today's conflict error and enters the retry/requeue path (§6.5).

The **job-config epoch remains a hard, all-or-nothing check**: it is what
serializes two plans for the *same* job config, and candidate lists must
not weaken it.

Race resolution is automatic because applies are serialized (one write
transaction locally; log order under raft) and each walk runs against
*committed* state: racer 1 binds A; racer 2 sees A's epoch bumped and
slides to B; racer 3 to C. Identical lists self-resolve — no
coordination, no retry.

Determinism: the walk is a pure function of the submission and committed
state — no clocks, no randomness — so raft replay on every peer binds the
same candidate. All randomness lives at *plan* time (§6.2), on the
leader, before the submission is created.

### 6.2 Candidate list construction: argmax head, jittered tail

Rank 0 is today's pick — the true argmax (with today's exact-tie
shuffle). Ranks 1..k−1 are **sampled without replacement,
score-weighted, from the near-tie band** (nodes within
`RESCHEDULE_SCORE_JITTER_THRESHOLD` of the best score; if the band is
thinner than k, extend down the ranking in score order).

Why the split, not jitter on the pick itself:

- **Uncontended plans pay zero quality tax** — they get their genuine
  first choice, which plain Fresh-mode jitter cannot promise.
- **Contended fallbacks decorrelate.** Racers marching down one shared
  prefix need k ≥ burst size (each racer consumes one slot); sampled
  tails spread racers across the whole near-tie band, so a short list
  (default k = 8) survives bursts much larger than k. This is Sparrow's
  power-of-k with a deterministic head.
- It also blunts fallback crowding: because the epoch skip is
  conservative ("touched" ≠ "full"), a burst would otherwise concentrate
  fallback traffic on the same next-best nodes and bump their epochs
  continuously.

The scored list this needs is already computed and discarded by
`select_cached_node`; construction is O(k) on top of today's scoring.

Known accepted quirk: under a burst, epoch-skips *spread* allocations
across the band even where bin_pack would happily have stacked several
on one node. The v2 answer, if metrics ever justify it, is
capacity-aware skipping (Phase 4) — not more jitter.

### 6.3 Config surface

One `schedulerSettings` key, defaulting on:

```jsonnet
{
  schedulerSettings: {
    // Shorthand: candidates per placement. 0 disables (today's behavior).
    plan_candidates: 8,

    // Full form when the defaults need overriding:
    // plan_candidates: {
    //   count: 8,       // list length k per placement
    //   band: 0.01,     // score band for tail sampling
    // },
  },
}
```

`plan_conflict_max_retries` and the backoff settings keep their meaning
(attempts are now rarely spent). One new tuning value,
`plan_conflict_requeue_delay_ms` (default 1000, jittered), paces the
requeue backstop (§6.5).

### 6.4 Multi-placement plans: coupling components

Two placements in one plan are **coupled** when the walk could make their
combined result invalid:

1. **Candidate-list overlap** — both lists contain node X. The planner's
   fit math for X assumed at most one of them lands there; two fallbacks
   onto X could oversubscribe it. (Static ports are a special case of
   the same node-sharing hazard.)
2. **Shared `distinct` constraint key** — `DistinctTracker` couples their
   choices. Note the coupling is on the *attribute*, not the node: two
   disjoint nodes can share a zone, so distinct-zone placements are
   coupled even with disjoint lists.
3. **Same co-placement group** — the group is one selection unit already.

Build the graph, take connected components:

- **Singleton components get the candidate walk** (§6.1),
  independently. This covers the motivating workload outright — a
  single-task sandbox job is one singleton — and the genuinely new
  multi-placement win: a GPU task and a CPU sidecar constrained to
  different pools have disjoint lists and each falls back independently.
- **Components of size >1 keep today's all-or-nothing epoch CAS** on
  their exact chosen nodes. A count-3 service on one pool is one
  component and behaves exactly as today. Correct and conservative;
  loosening it (striping disjoint lists across identical replicas,
  per-component partial commit) is Phase 4.

Colocation affinities couple placements only in score, not validity —
they are deliberately **not** a coupling edge. A fallback can land a
task one node away from its preferred partner; that is a quality cost
under contention, not a correctness one, and it is the same cost a
conflict-retry replan pays today.

### 6.5 The backstop: conflict exhaustion requeues, never fails

Independently of candidates: when `plan_with_conflict_retries` exhausts
its budget, the eval is **released back to pending with a delay**
(reusing `release_eval_for_retry`, `store.rs:9540`) instead of being
terminally failed. Conflict is transient by definition — treating it as
permanent failure is how today's sharp edge ("sandbox never starts")
exists. Terminal failure remains for non-conflict errors.

The requeue emits the existing eval lifecycle events, so a pool at
saturation shows as visibly pending-and-retrying work, not silence.

### 6.6 Observability

- `plan.applied` events and the apply outcome gain
  `candidateRank` per placement (0 = first choice) and a
  `fallbacks` count per plan.
- `legion job plan` explain output gains the candidate list preview per
  task (name-ranked, same envelope discipline as the index's `index`
  field).
- Prometheus: `legion_scheduler_plan_candidate_rank` (histogram),
  `legion_scheduler_plan_apply_total{outcome}` gains a `fallback`
  outcome label value, and a counter for conflict requeues.
- Plan spans gain `candidate_rank` alongside the existing stable IDs.

## Proto, Struct, Config, and Persistence Changes

- **Proto:** none. No job-spec or RPC shape changes in v1; `job plan`'s
  explain payload is the existing JSON envelope.
- **Manager config:** `schedulerSettings.plan_candidates`
  (int shorthand or `{count, band}`; default `8`),
  `schedulerSettings.plan_conflict_requeue_delay_ms` (default 1000).
  Nothing in `configs/jobs/maximal.jsonnet` — there is no job-config
  surface.
- **Store:** `plan_submission.placements` JSON becomes
  `{"format": 2, "placements": [{"task_id": …, "candidates": ["node-a", …]}]}`;
  the v1 shape (`[[node_id, task_id], …]`) remains readable forever
  (rows are short-lived, but restart can observe queued rows).
  `expected_version_vector` covers the union of candidate lists plus the
  job-config scope. Apply outcome carries the bound node + rank per
  placement. No schema migration — the column is already free-form JSON.
- **Scheduler:** candidate-list construction in the selection path
  (`select_cached_node` and the policy path return ranked lists),
  component partitioning in `build_plan`, submission narrowing widened
  to the candidate union.
- **Eval:** exhaustion path switches from `complete_eval(failed)` to
  `release_eval_for_retry`.

## Implementation Phases

Each phase ships and stands alone.

- **Phase 0 — Measure.** A contention benchmark and integration harness:
  N-way concurrent single-allocation submits against one pool, recording
  conflict rejections, retry rounds, dropped evals, and time-to-all-
  placed, at N ∈ {8, 32, 128} × pool sizes {small, headroom}. Baseline
  recorded in `benchmarks/scheduler/README.md` (the AGENTS.md
  before/after rule). Doubles as the raft-mode concurrent-planning
  regression test already tracked in `TODO.md`.
- **Phase 1 — Never drop work.** The requeue backstop (§6.5). Small,
  independently valuable, and the safety net every later phase assumes.
- **Phase 2 — Candidate lists for single-placement plans.** Submission
  format v2, the epoch walk with per-candidate re-validation and
  port-collision skip, argmax-head/jittered-tail construction,
  `plan_candidates` setting and kill switch, observability (§6.6).
  Multi-placement plans submit format v2 with singleton lists and keep
  all-or-nothing semantics. This is the headline: the sandbox burst is
  fixed here.
- **Phase 3 — Coupling components.** Partition multi-placement plans;
  singleton components walk independently; coupled components unchanged.
  Ships the heterogeneous-job win (disjoint-pool tasks fall back
  independently).
- **Phase 4 — Backlog (each optional, separately justified):**
  capacity-aware candidate skipping (applier-side ledger for the
  touched-but-fits case); per-component partial commit + eval refill
  (Omega's incremental transaction, riding the Reschedule refill
  machinery); striped disjoint candidate lists for identical replicas;
  a serialized or batched apply lane for physically-full pools if
  Phase 0/2 metrics show sustained all-candidates-stale conflicts.

## Testing Plan

- **Unit (Phase 2):** walk semantics — first fresh candidate binds,
  stale/unavailable/port-collision candidates skip, all-stale rejects;
  job-config epoch mismatch still rejects regardless of candidates;
  tail sampling (seeded RNG: head is argmax, tail ⊆ band, no
  duplicates); v1/v2 placements parsing.
- **Unit (Phase 3):** component partitioning — overlap edges, distinct-
  key edges (including distinct-on-attribute with disjoint node lists),
  group edges, transitivity; singleton detection.
- **Integration (Phases 1–3):** the Phase 0 burst harness with
  candidates on — zero failed evals, conflict rejections ≈ 0 at
  headroom, all allocations resource-valid; saturation scenario —
  exhausted plans requeue and place as capacity frees; the fallback-
  stacking regression — two coupled placements whose lists share a node
  must not both land on it (proves component gating);
  `concurrent_plans_cannot_oversubscribe_a_node` and the epoch-fencing
  suites unmodified and green with `plan_candidates` on and off.
- **Raft:** replay determinism — a submission applied on the leader and
  replayed on followers binds identical candidates; mixed-format rows
  across restart.
- **Benchmarks:** Phase 0 matrix before/after Phase 2 (numbers in the
  PR, per AGENTS.md); a no-contention guard proving single-planner
  plan/apply cost is unchanged within noise.

## Rollout and Backward Compatibility

- No wire or job-spec changes; no store migration (JSON column, format
  tag, v1 shape readable forever).
- `plan_candidates: 0` restores today's single-node submissions and
  all-or-nothing apply byte-for-byte; `plan_candidates` defaults on
  because the semantics are conservative by construction (a candidate
  list of length 1 *is* today's plan).
- **Raft version skew is the one real rollout edge:**
  `apply_plan_submission` is a replicated op, and an old-version peer
  replaying a format-2 submission would fail to parse it. This rides the
  existing manager upgrade-order policy
  ([docs/upgrades.md](../../docs/upgrades.md)) — same-version managers —
  and the parse failure is loud, not divergent. For operators who need
  to run mixed versions longer than an upgrade window,
  `plan_candidates: 0` holds format-2 emission off until the fleet is
  level. See open question below.
- Phase 1's requeue changes eval terminal behavior: evals that today end
  `failed` after conflict exhaustion instead stay pending with backoff.
  That is the intended fix; operators watching for failed evals see
  retry events instead.

## Open Questions

- **Default k (lean: 8).** Payload is bytes per row and the walk is
  O(k) epoch lookups in-transaction; 8 covers bursts far beyond 8
  thanks to tail sampling. Phase 0 data picks the final number.
- **Tail sampling weights (lean: proportional to score within the
  band).** Uniform is simpler; score-weighted keeps the quality
  ordering meaningful. Cheap to change; settle with Phase 2 benchmarks.
- **Should format-2 emission self-gate on cluster version rather than
  the settings knob (lean: no for v1)?** A min-peer-version check is
  nicer UX but adds a version-negotiation surface for a window the
  upgrade policy already covers. Revisit if mixed-version fleets turn
  out to be common.
- **Requeue forever or cap (lean: forever, with capped backoff).**
  Conflict means capacity churn; the honest states are "placed" or
  "visibly waiting." A terminal state after M requeues reintroduces
  silent drops under a different name. Revisit if requeue storms show
  up in practice.
- **Does the near-full pool eventually need a real second lane
  (serialized or batched apply) (lean: decide from Phase 2 metrics)?**
  Candidates fix artificial contention (headroom, correlated picks),
  not physical contention (one node fits). If
  `plan_apply_total{rejected}` stays materially nonzero on saturated
  pools after Phase 2, that is the signal — and a separate plan.

## Acceptance Criteria

- **Phase 0:** the contention matrix runs in CI-friendly form; baseline
  numbers (conflict rejections, dropped evals, time-to-all-placed)
  recorded in `benchmarks/scheduler/README.md`; the raft-mode concurrent
  planning path is exercised by a test.
- **Phase 1:** a submit eval that exhausts conflict retries is observably
  requeued (event + pending row) and eventually places when capacity
  allows; no eval reaches `failed` via the conflict path; existing
  non-conflict failure behavior unchanged.
- **Phase 2:** the Phase 0 burst at headroom shows zero failed evals and
  conflict rejections reduced to ~0 (target: ≥95% of plans bind within
  rank ≤ 3 at N=32 on the benchmark fleet); uncontended placements bind
  rank 0 (equivalence with today's argmax); oversubscription and
  epoch-fencing suites green unmodified; `plan_candidates: 0` restores
  baseline behavior in an integration test; `job plan`, events, and
  metrics show ranks truthfully.
- **Phase 3:** heterogeneous multi-placement plans (disjoint pools) fall
  back independently under injected contention; the fallback-stacking
  regression proves coupled components never walk; count-N same-pool
  services behave exactly as Phase 2.
