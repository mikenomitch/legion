# Judgement Calls: Late-Binding Plan Apply

Decisions behind
[`plans/late-binding-plan-apply-plan.md`](../plans/late-binding-plan-apply-plan.md).
**Entries are ranked by project impact, highest first; the number is the
rank.** Seeded at design time (2026-07-30), before execution — deviations
during implementation get appended at their rank, per
[README.md](README.md).

## 1. "Free at apply time" is defined by the epoch, not by capacity math

**Call.** The applier binds a placement to the first candidate whose
*node epoch* is unchanged since the planner sampled it. An unchanged
epoch proves no allocation changed on that node, so the planner's
plan-time fit verification is still valid — the epoch is the fit proof,
carried forward. The applier never reconstructs a `ResourceLedger`.

**Alternatives.** (a) An applier-side ledger: recompute free capacity
inside the write transaction and bind to the first candidate that
*actually* fits. Strictly better placements (a touched-but-still-fitting
node is not skipped) and the shape a scheduler purist would build.
(b) No walk at all — keep single-node plans and rely on plan-time
decorrelation (jitter) alone.

**Trade-off.** Conservative skips: an unrelated small allocation on
candidate A bumps its epoch and pushes the plan to B even though A still
fits, so bursts spread across the near-tie band where bin_pack would
have stacked. Accepted, and bounded by the band construction — every
candidate is a near-equivalent by score. Evidence to revisit: fallback-
rank metrics showing deep-rank binds while pool utilization is low
(capacity-aware skipping is the named Phase 4 follow-up).

**Why (simplicity, performance).** (a) moves scheduling logic into the
store's replicated apply path — the `ResourceLedger`, resource policy
normalization, and reservation handling would all grow a second home
inside a raft state machine, permanently. The epoch walk adds only
per-candidate comparisons to a transaction that already does epoch
comparisons, keeps the applier explainable in one sentence ("first
candidate nobody touched"), and is trivially deterministic for raft
replay. (b) loses the headline property: with commit-time choice, a race
costs a rank; with jitter alone it still costs a full replan.

## 2. Multi-placement plans partition into coupling components; v1 walks only singletons

**Call.** Placements in one plan are grouped by coupling edges —
overlapping candidate lists (capacity/static-port sharing), shared
`distinct` constraint keys, same co-placement group. Singleton
components get independent candidate walks; components of size >1 keep
today's all-or-nothing CAS on their exact nodes. Colocation affinities
are deliberately not an edge (score coupling, not validity coupling).

**Alternatives.** (a) Candidate lists for every placement with
applier-side stacking checks — maximal fallback coverage, requires the
applier ledger rejected in call 1. (b) The blunt scope: candidate lists
only for plans with exactly one placement, everything else unchanged —
simpler, but a GPU task + CPU sidecar in disjoint pools would forfeit
late binding for no correctness reason.

**Trade-off.** A count-N service on one pool is one coupled component
and gains nothing from this plan until Phase 4 (striped disjoint lists,
partial commit). Accepted: those plans are exactly where intra-plan
stacking can oversubscribe a node through epoch checks that cannot see
it, and the motivating workload (independent single-task jobs) is all
singletons anyway.

**Why (simplicity).** The component rule preserves the invariant that
makes optimistic apply trustworthy — the epoch CAS never admits a
placement whose fit proof might be invalidated *by the same plan*. One
graph with three edge kinds is explainable and unit-testable; every
loosening (striping, partial commit) is an explicit later decision
rather than a hidden assumption.

## 3. Conflict-exhausted evals requeue with backoff; they never terminally fail

**Call.** When `plan_with_conflict_retries` exhausts its budget, the
eval is released back to pending with a delay (reusing
`release_eval_for_retry`, built for foreign-lane `NotRaftLeader`
rejections) instead of being marked `failed` — a state that is never
retried (`src/storage/store.rs:9525`). Terminal failure remains for
non-conflict errors.

**Alternatives.** (a) Keep terminal failure but raise the default retry
budget — cheaper, but any finite in-handler budget still drops work
under a large enough burst, just later. (b) A capped requeue count with
a terminal state after M rounds — reintroduces the silent drop under a
different name.

**Trade-off.** A plan that can never place (saturated pool) now retries
forever at capped backoff instead of parking in a terminal state; the
queue carries standing work and operators watching for `failed` evals
must watch pending-age instead. Accepted: conflict is transient by
definition, and "placed" or "visibly waiting" are the only honest
states for acknowledged work. Evidence to revisit: requeue storms
dominating the eval queue in practice.

**Why (simplicity/ease).** "A submit either places or is visibly
pending" is a guarantee an operator — or an agent driving `job run` —
can rely on without building their own poll-and-resubmit loop. The
current behavior (three lost races ⇒ job silently never starts) is the
sharpest edge this whole plan exists to remove; candidate lists make it
rare, this makes it impossible.

## 4. Candidate lists have a deterministic argmax head and a jittered tail

**Call.** Rank 0 is today's pick — the true argmax with exact-tie
shuffle. Ranks 1..k−1 are sampled without replacement, score-weighted,
from the near-tie band (reusing the 1% `RESCHEDULE_SCORE_JITTER_THRESHOLD`
band). All randomness runs at plan time on the leader; the apply walk is
deterministic.

**Alternatives.** (a) Arm the existing Reschedule-mode score jitter in
Fresh mode — five lines, but taxes *every* placement's quality to fix a
problem that only exists under contention, and a race still costs a
full replan. (b) Pure top-k by score, no jitter — self-resolves (racers
slide down the shared prefix against committed state) but needs
k ≥ burst size, since each racer consumes one shared prefix slot, and
concentrates fallback traffic on the same next-best nodes.

**Trade-off.** Fallback placements are randomized within the band rather
than strictly score-ordered — under contention you may get the 4th-best
near-tie instead of the 2nd. Accepted: the band bounds the quality loss
by construction, and fallbacks only occur when a race actually
happened.

**Why (performance).** Sparrow's power-of-k result: decorrelated tails
let a short list (k=8) survive bursts far larger than k, keeping the
submission payload and walk cost O(k) while conflict probability under
a burst drops to ~P(all k candidates touched in the snapshot-to-apply
window). Better quality than (a) when uncontended, better burst
survival than (b) at equal k.

## 5. Per-component partial commit is deferred to the backlog, not v1

**Call.** A rejected component (or all-candidates-stale plan) rejects
and re-enters the retry/requeue path whole; the apply transaction does
not commit surviving components and report the rest. Omega-style
incremental commit is Phase 4, riding the existing Reschedule refill
machinery if metrics justify it.

**Alternatives.** Partial commit in v1: one transaction inserts the
placements that resolved and returns the conflicted remainder for
refill — strictly less wasted work, and the refill shape
(`count.per` group refill, excluded nodes) already exists.

**Trade-off.** A multi-component plan that loses one component replans
everything, including placements that were never in dispute. Accepted
for v1: with candidate walks, component-level rejection is already the
rare path, and partial commit changes plan-level semantics (a "half
applied" submission state) that every consumer of plan outcomes —
events, rollouts, `job plan` explain, idempotency — would need to
learn.

**Why (simplicity).** Plan apply currently has two outcomes; keeping it
that way while the walk absorbs the common races keeps the mental model
("a plan applies or it retries") intact. Adding a third outcome is
justified only by evidence the two-outcome model still wastes real
work.

## 6. Physically-full pools stay out of scope — no serialized lane in this plan

**Call.** When only one node fits, candidate lists degenerate to length
1 and behavior is today's plus the requeue backstop. The "second lane"
(per-pool serialized or batched apply) is explicitly not built here; the
plan names the metric (`plan_apply_total{rejected}` sustained nonzero on
saturated pools after Phase 2) that would trigger a separate plan.

**Alternatives.** Build batching now: coalesce pending single-task
submits against one pool into a single plan pass — one snapshot, one
CAS, zero mutual conflicts; it is what Nomad's single-scheduler shape
gives implicitly.

**Trade-off.** Saturated-pool bursts still pay retry/requeue rounds
rather than queueing cleanly. Accepted: at saturation the binding
constraint is capacity, not scheduler throughput — the work waits either
way, and the requeue backstop makes the waiting visible and safe.

**Why (simplicity, extensibility).** Batching introduces a second
scheduling mode with its own fairness, ordering, and failure questions.
Making the optimistic lane cheap first (this plan) and measuring what
remains keeps Legion at one scheduling concept unless data proves two
are needed — the differentiator claim is "simplest scheduler to run,"
not "most lanes."
