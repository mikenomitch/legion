# Judgement Calls: Scheduler Sharding Strategies (proposal)

Decisions made while writing
[`../plans/scheduler-sharding-strategies-proposal.md`](../plans/scheduler-sharding-strategies-proposal.md).
This is a proposals document, so these are calls about *which designs get
recommended and how the option space is framed* — the same class of calls the
scheduler-architecture-review doc recorded. Entries are ranked by project
impact, most impactful first; the number is the rank.

## 1. Node-owned (pool-keyed) sharding is the recommended axis, not job-keyed shared-state sharding

**Call.** The proposal ranks M4 — scheduler shards that *own disjoint node
sets* (keyed by pool in v1), with jobs routed to the shard owning their pool —
as the in-cluster scaling axis, and rejects the Omega/Gödel shape (jobs hash
to shards, any shard places on any node, optimistic cross-shard arbitration)
outright rather than presenting the two as peers.

**Alternatives.** Job-keyed sharding is the version a reasonable engineer
would pick first: it needs no node partitioning, jobs never straddle shards,
it is the published shape (Omega, Gödel, the K8s multi-scheduler KEP), and
Legion's partitioned epochs + late binding look purpose-built for it.

**Trade-off.** Pool-keyed ownership is coarser and operator-shaped: shard
balance follows pool sizes, a job spanning pools on different shards is
rejected at submit (`pools "web" and "batch" map to scheduler shards 0 and 2;
pin them to one shard or split the job`), and a single giant pool needs the
v2 hash-bucket extension before it can split. Accepted: less elegant load
spreading in exchange for keeping the correctness argument. Revisit if a real
deployment shows pool-shaped sharding chronically unbalanced *and* the v2
bucket split doesn't cover it.

**Why (performance without re-litigating correctness).** Under row-changeset
replication, single-writer ownership is what makes replicas provably
byte-identical (`src/storage/raft/ownership.rs:5`), and apply-time
revalidation through one log is what makes oversubscription impossible rather
than repairable. Job-keyed shards put every shard's log on every node's
capacity rows — the exact multi-writer case the ownership guard exists to
reject — and the honest fixes collapse back into a single apply arbiter
(which is M3) or accept-and-repair eviction (which breaks a stated
invariant). Node ownership gets the same throughput multiplication while the
invariant survives *by construction*. Simplicity also weighs in: "the batch
pool has its own scheduler" is explainable at 3am; "shards race and the loser
re-plans, unless both won, which cannot happen because—" is not.

## 2. The capture/apply mutex is named as the honest ceiling of in-cluster sharding

**Call.** The proposal states up front that `CAPTURE_MUTEX`
(`src/storage/raft/capture.rs:133`) serializes SQLite capture and apply
across *all* raft groups, so shard scaling is near-linear only until
aggregate local write work saturates roughly one core — and it sequences the
per-shard-state-DB fork (the fix) as v2/v3, gated on measuring that mutex as
the wall.

**Alternatives.** (a) Present shards as linearly scaling and let the
implementation discover the shared-writer ceiling — the shinier pitch, and
what the lanes A/B numbers alone would suggest. (b) Require per-shard state
DBs from v1 so the ceiling never appears — architecturally purer, and it
would also make row-scoped snapshots trivial.

**Trade-off.** (a) was never really on the table after phase 3's lesson
(unmetered waits hide inside whoever holds the stopwatch). Against (b): v1
on a shared DB accepts a known future ceiling and a second migration later,
in exchange for shipping without rewriting every read path in `Store`
(~150 replicated call sites plus every direct SQL read). Accepted: the v2
fork may prove necessary sooner than hoped; the Phase 0 capture-mutex-wait
histogram is the tripwire.

**Why (performance, via honesty).** Group commit means each batched op holds
the mutex for µs–ms of local work while pipelines hold quorum RTTs — the
same asymmetry that made lanes win despite the shared mutex — so the shared
DB has real runway, and claiming more than that would repeat the phase-2
mistake of optimizing against an unmeasured wall.

## 3. More write-class lanes are refused as a scaling axis, permanently

**Call.** M1 (a KV lane, an SD lane, a rollout lane, …) is marked *refuse* —
not deferred, refused — and the three existing lanes are declared the
complete vertical split.

**Alternatives.** Keep the door open ("add lanes as write classes emerge"),
which is the natural reading of how lanes were built and the path of least
resistance every future feature plan would take.

**Trade-off.** A future feature with a genuinely new, high-volume write class
has to argue against a standing "refuse" instead of into an open door.
Accepted deliberately: the multi-raft plan's §1a already fought this fight
for service discovery and concluded lane-per-feature is the failure mode.

**Why (simplicity).** Each lane is a leader, an election domain, a snapshot
pipeline, a reconciler set, and a permanent row in the operator's mental
model — and buys zero state-DB concurrency past the shared mutex. The write
classes are ~4 and enumerable; the residue ceiling (`ID_STRIDE = 8`,
`src/storage/raft/mod.rs:73`) is 8. Horizontal shards within a class scale;
vertical lanes per feature only widen.

## 4. Everything is sequenced behind a measured post-batching ceiling (Phase 0)

**Call.** No sharding method graduates to an implementation plan until the
fast native submitter exists and a re-measured ceiling *names its
bottleneck* (pipeline wait vs capture/apply vs planner). The proposal ships
a map, not a mandate.

**Alternatives.** Graduate M4 v1 now — the phase-2/3 evidence (3.07 ms/op
scheduler-lane pipeline wait, apply = 67% of plan time) already points at
the pipeline, and waiting costs calendar time.

**Trade-off.** If Phase 0 confirms the pipeline, the wait bought nothing but
confidence. Accepted, because the last three campaigns each *overturned* the
previous session's diagnosis (conflicts → read pool → pipeline → driver),
and phase 5 ended driver-bound with the ceiling unlocated — the exact
situation in which committing to a specific shard design has the worst
expected value.

**Why (performance discipline).** The repo's own precedent:
`eval-thread-default-plan.md` deliberately picks no number for the same
reason. A sharding design tuned against a wall nobody has hit inherits the
adaptive-concurrency plan's warning about tuning controllers against
distributions that no longer exist.

## 5. v1 resharding is a declared format break (JC-43), not an online operation

**Call.** Enabling or resizing `raft.scheduler.shards` in v1 takes the
existing topology-change path — backup → fresh cluster → restore — recorded
in the `topology.json` stamp exactly like `single`↔`lanes`. Online
resharding (fixed virtual buckets, bucket handoff by snapshot install) is
specified as v2, not v1.

**Alternatives.** Virtual buckets from day one, so shard count is never a
restart-the-world event. TiKV/CockroachDB prove the shape works.

**Trade-off.** An operator who under-provisions shards pays a maintenance
window to grow — a real operational cost, stated in the proposal's scenario
2 rather than hidden. Accepted: the bucket-handoff machinery (row-scoped
install into a *different* group, mid-flight eval routing, cutover fencing)
is the single most delicate part of the whole design, and v1 shipping
without it is the difference between one release and three.

**Why (simplicity/ease of shipping, trading against ease of growth).** The
JC-43 rule already exists, is documented, is tested, and operators of raft
mode already live under it for topology; reusing it keeps v1's new-mechanism
budget spent on the one thing that must be right (row-scoped ownership and
snapshots). Ease-of-growth lost to shippability; the v2 path is named so the
loss is visibly temporary.

## 6. Federation is positioned as complementary, not competing

**Call.** M6 (more clusters) is presented as the *outer* axis that scales
every dimension including per-manager memory, with M4's job described as
raising the single-cluster ceiling so federation gets chosen for geography,
tenancy, and blast radius rather than forced at 5k nodes for throughput.
The proposal does not re-litigate any of the federation plan's design.

**Alternatives.** Treat federation as the answer to the whole question
("need more throughput? run more clusters") and skip in-cluster sharding —
Borg's actual answer, and the simplest possible engineering position.

**Trade-off.** Maintaining both axes is more total machinery than picking
one. Accepted: they fail differently — federation cannot bin-pack across
clusters (its own stated non-goal), and sharding cannot shrink the global
read-model mirror — so each covers the other's hole.

**Why (simplicity for the operator, not the maintainer).** Forcing
federation for throughput makes the *operator* absorb N control planes to
solve what is, at that scale, a Legion implementation limit. The competitive
claim ("scales better than Nomad/K8s") reads on a single cluster; conceding
it at the point where one raft group saturates would concede it exactly
where the incumbents are weakest too.
