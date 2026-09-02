# Judgement Calls: Scheduler Sharding Strategies (proposal)

Decisions made while writing
[`../plans/scheduler-sharding-strategies-proposal.md`](../plans/scheduler-sharding-strategies-proposal.md).
This is a proposals document, so these are calls about *which designs get
recommended and how the option space is framed* — the same class of calls the
scheduler-architecture-review doc recorded. Entries are ranked by project
impact, most impactful first; the number is the rank. Entries 1 and 2 are
**reversals** of revision 1's recommendations and say what the symptom was.

## 1. A shard is a raft group plus its own state file — not a raft group over the shared `store.sqlite` (reversal)

**Call.** `raft.scheduler.shards = N` creates N scheduler groups *and* N
state files (`scheduler-<k>.sqlite`), each with its own write connection and
capture mutex. Control and status stay in `store.sqlite`; read connections
`ATTACH` the shard files and present the scheduler tables through `TEMP`
`UNION ALL` views so existing read SQL keeps working.

**What revision 1 said, and why it was wrong.** Revision 1 put N groups over
the one shared file and called the scaling "near-linear until aggregate
local write work saturates one core", deferring per-file partitioning to a
v2/v3 fork. Re-reading the pipeline showed the cycle is strictly serial per
group — capture, `block_on(client_write)`, reply (`src/storage/raft/driver.rs:1499`,
`:1535`) — with both capture and apply under the process-global mutex
(`src/storage/raft/capture.rs:200`, `:522`). N groups over one file overlap
only the quorum round-trip, and after the batching work the local body of a
batch (~20 ms at depth, phase 5) dwarfs a same-AZ round-trip (1–3 ms). The
arithmetic (`N_max ≈ T / (W_c + W_a)`) comes out near **1.1** — a latency
feature on WAN quorums, not a throughput feature. The lanes A/B that
suggested otherwise measured *unbatched* floods, where the round-trip
dominated; batching moved the regime and revision 1 did not re-derive.

**Alternatives.** (a) Revision 1's shared-file shards. (b) An in-memory
scheduler state machine (D3), which shrinks the local work to microseconds
and would let shared-file groups scale — rejected by the architecture review
for operability, and it reaches the same parallelism with a worse story.
(c) Pipelined captures within one group with per-shard applied indexes —
re-implements what independent groups provide, and breaks "captures run on
applied state".

**Trade-off.** A multi-file `Store` is real surgery (per-file connections and
migrations, union views, cross-file joins), cross-lane `ON DELETE CASCADE`
constraints become unenforceable across files (they are already avoided by
composite ordering; an audit test is the deliverable), `scheduler_event`
rows become shard-resident, and the default attach limit caps v1 at 8
shards. Accepted: the alternative was shipping a knob that measurably does
nothing at throughput. What would revisit it: a Phase 0 ratio showing local
work at under a tenth of the cycle on the target hardware.

**Why (performance, and simplicity by subtraction).** The unit that has to
be multiplied is the SQLite writer, and SQLite is single-writer *per file*
(TiKV's shared-engine multi-raft scales only because RocksDB is
multi-writer). Per-file shards also delete revision 1's biggest new
mechanism — row-scoped snapshot dump/install — because the whole-file
`VACUUM INTO` path already exists (`driver.rs:2795`), and they make
ownership *physical*: a shard's connection cannot reach another shard's
rows, so the guard stays table-granular and the only new check is routing.
Fewer new mechanisms for a real multiplier is the better trade.

## 2. v1 keeps every shard leader on the anchor manager; spreading leadership is rung B (reversal)

**Call.** Rung A leaves the convergence policy alone: the control-lane
anchor leads all N shards, runs N pipeline threads over N files, and claims
per shard from one shared `evalThreads` pool. Spreading shard leadership
across managers is rung B, shipped when a single manager's cores or fsync
stream measurably saturate.

**What revision 1 said, and why it was wrong.** Revision 1 made spread the
design point of v1 ("the inverse of today's convergence policy"), which
pulled the composite forwarding fast path, the `legion state backup` fan-in
rewrite, and a shard-aware `readyz` into the first shippable phase. With
per-file shards (entry 1), co-location already multiplies writers and
pipelines on one box — the measured wall — so spread was paying a large
operational bill for the *next* wall (planner CPU across machines), which
nothing has measured yet.

**Alternatives.** Spread in v1 (revision 1); or M3 plan-shipping as the
cross-machine planner multiplier instead of spread (a wire surface and
staleness handling, and it moves only planning, not apply or fsync).

**Trade-off.** Rung A's ceiling is one machine; an operator whose leader box
saturates under N shards has to wait for rung B. Accepted: the 32-thread
local sweep was still rising, so one box has planner headroom, and every
composite, reconciler, convergence rule, and backup precondition keeps
working *unchanged* under co-location. What would revisit it: a rung-A
deployment showing the leader CPU-bound with pipelines idle.

**Why (simplicity).** Deliberate divergence was made *survivable* by the
standing-split work; making it the *design point* is a different, larger
change (forwarding as the fast path, backup without an all-groups leader).
Sequencing it behind evidence keeps v1 to the one new mechanism — files —
and inherits today's leadership model for free.

## 3. Node-owned (pool-keyed) sharding, not job-keyed shared-state sharding

**Call.** Shards own disjoint node pools; a job lives in the shard that owns
its pool. The Omega/Gödel shape (jobs hash to shards, any shard places on any
node, optimistic cross-shard arbitration) is rejected outright.

**Alternatives.** Job-keyed sharding is the version a reasonable engineer
picks first: no node partitioning, jobs never straddle shards, it is the
published shape, and the partitioned epochs + late binding look
purpose-built for it.

**Trade-off.** Pool-keyed ownership is coarser and operator-shaped: shard
balance follows pool sizes, a job spanning pools on different shards is
rejected at submit (`pools "web" and "batch" map to scheduler shards 0 and 2;
pin them to one shard or split the job`), capacity does not pool across
shards, and a single giant pool needs rung C's sub-shards before it can
split. Accepted: less elegant load spreading for keeping the correctness
argument. Revisit if a real deployment is chronically unbalanced *and* rung
C does not cover it.

**Why (performance without re-litigating correctness).** Under
row-changeset replication, single-writer ownership is what makes replicas
provably identical (`src/storage/raft/ownership.rs:5`), and apply-time
revalidation through one log is what makes oversubscription impossible
rather than repairable. Job-keyed shards put every shard's log on every
node's capacity rows; the honest fixes collapse into one apply arbiter
(M3) or accept-and-repair eviction (breaks a stated invariant). Node
ownership gives the same multiplier with the invariant intact *by
construction* — and "the batch pool has its own scheduler" is explainable
at 3am.

## 4. More write-class lanes are refused as a scaling axis, permanently

**Call.** M1 is marked *refuse*, not deferred; the three lanes are the
complete vertical split.

**Alternatives.** Keep the door open — "add lanes as write classes emerge" —
the path of least resistance for every future feature plan.

**Trade-off.** A future feature with a genuinely new high-volume write class
argues against a standing refusal. Accepted deliberately: the multi-raft
plan's §1a already fought this for service discovery.

**Why (simplicity).** Each lane is a leader, an election domain, a snapshot
pipeline, a reconciler set, and a permanent row in the operator's mental
model — and over the shared file buys zero write concurrency (entry 1's
arithmetic applies to lanes exactly as to shards). The write classes are ~4
and enumerable; the residue ceiling (`ID_STRIDE = 8`,
`src/storage/raft/mod.rs:73`) is 8. Horizontal shards within a class scale;
vertical lanes per feature only widen.

## 5. Everything is sequenced behind a measured, attributed ceiling — and Phase 0 is made nearly free

**Call.** No method graduates until Phase 0 names the bottleneck. Revision 2
makes Phase 0 cheap: the capture-vs-propose split is already metered
(`legion_raft_capture_seconds` / `legion_raft_propose_apply_seconds`,
`src/manager/telemetry.rs:765`) so the ratio comes from archived campaigns,
plus one in-process micro-benchmark of N pipelines over one file versus N
files. The native submitter remains the gate for the *production* number,
not for the design decision.

**Alternatives.** Graduate rung A now — phase 2/3's evidence already points
at the pipeline, and waiting costs calendar time.

**Trade-off.** If Phase 0 confirms the pipeline, the wait bought only
confidence. Accepted: the last three campaigns each overturned the previous
session's diagnosis (conflicts → read pool → pipeline → driver), and this
document's own revision 1 was overturned by one afternoon of arithmetic.

**Why (performance discipline).** The repo's precedent —
`eval-thread-default-plan.md` deliberately picks no number — and the
adaptive-concurrency plan's warning about tuning against distributions
that no longer exist.

## 6. v1 resharding is a declared format break (JC-43); online resharding is rung C

**Call.** Changing `shards` or `assignments` in v1 takes the existing
topology-change path — backup → fresh cluster → restore — recorded in the
`topology.json` stamp. `legion raft shard move-pool` / `shard add` are
rung C.

**Alternatives.** Online resharding from day one; TiKV/CockroachDB prove the
shape.

**Trade-off.** Under-provisioned shards cost a maintenance window to grow.
Accepted: with per-file shards the rung-C mechanism is *simpler* than
revision 1's bucket handoff (a pool move is a whole-file snapshot of the
pool's rows into another group plus a map flip), which makes deferring it
cheap rather than risky.

**Why (ease of shipping over ease of growth).** The JC-43 rule exists, is
documented and tested, and raft-mode operators already live under it for
topology; reusing it keeps v1's new-mechanism budget on the multi-file
store.

## 7. `shards = 1` is behaviorally identical, not byte-identical, under `lanes`

**Call.** Under `lanes`, scheduler state lives in `scheduler-0.sqlite` even
at N = 1, so there is one code path; the invariant is "passes the whole
existing raft battery", not "same bytes on disk". `single` topology is
untouched and stays byte-identical.

**Alternatives.** Keep scheduler tables in `main` at N = 1 and only move
them at N ≥ 2 — byte-identical for today's lanes users, at the cost of two
Store layouts and a second migration path.

**Trade-off.** Existing `lanes` clusters take a JC-43 break on the upgrade
that introduces shard files, even if they never set `shards`. Accepted:
`lanes` is opt-in and documented experimental, its on-disk shape is already
stamped, and one layout is the difference between one set of tests and two.

**Why (simplicity).** Two layouts is the kind of forked path that
accumulates bugs at the seam; the stamp makes the break loud rather than
silent.

## 8. Federation is positioned as complementary, not competing

**Call.** M6 is the *outer* axis that scales every dimension including
per-manager memory; M4's job is raising the single-cluster ceiling so
federation is chosen for geography, tenancy, and blast radius. The
federation plan's design is not re-litigated.

**Alternatives.** Treat federation as the whole answer and skip in-cluster
sharding — Borg's actual position, and the least engineering.

**Trade-off.** Two axes is more total machinery than one. Accepted: they
fail differently — federation cannot bin-pack across clusters (its own
non-goal) and sharding cannot shrink the global read model — so each covers
the other's hole.

**Why (simplicity for the operator, not the maintainer).** Forcing
federation for throughput makes the operator absorb N control planes to
solve a Legion implementation limit, and concedes the "scales better than
Nomad/K8s" claim exactly where the incumbents are weakest.
