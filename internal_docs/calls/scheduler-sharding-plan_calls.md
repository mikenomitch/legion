# Judgement calls — Sharded Scheduling

Decisions taken while writing
[`../plans/scheduler-sharding-plan.md`](../plans/scheduler-sharding-plan.md).
**Entries are ranked by project impact, highest first; an entry's number is
its rank.** Insert new entries at their rank rather than appending.

Nothing here has shipped — these are design calls made in the plan. Calls
made *during* implementation get appended at their rank, and any open
question resolved along the way becomes an entry with the alternative that
lost.

## 1. The shard key is pool subdivided by node hash, not pool alone

**Call.** A shard owns node groups keyed `(pool, hash(node_id) %
subdivisions)`, with `subdivisions` derived from node count.

**Alternatives.** (a) Pool alone — the intuitive key, already the default
`partition_by` of the scheduling index (`src/scheduling_index.rs:149`),
already the axis operators and ACLs reason about, and it needs no hashing
at all. (b) Hash of node id alone — perfectly even shards, no skew
handling needed. (c) Hash of *job* id, sharding the work rather than the
fleet.

**Trade-off.** The hash subdivision breaks the clean "a shard is a pool"
story, so an operator reading `legion operator scheduler` sees two shards
covering one pool and has to be told why. It also means shard membership
changes when the fleet changes size, which rebalance has to handle.

**Why (it is the difference between working and not).** Pool alone fails
the common case outright: most Legion clusters have exactly one pool
(`default`), which under (a) yields exactly one shard and no win — the
plan would ship a feature that does nothing for its median user. (b)
discards the locality the index already gives us and makes every
pool-bound query cross-shard. (c) shards the work but leaves every planner
reading the whole fleet, which is the cost this plan is trying to
partition. Keeping pool as the outer key preserves the operator-meaningful
boundary; the inner hash is what makes it apply to the cluster everyone
actually runs.

## 2. Shards are a contention device, never a correctness boundary

**Call.** Apply stays a single arbiter with today's per-node epoch CAS.
Shards partition *who plans what*, and nothing else. Work stealing across
shards is therefore allowed by construction.

**Alternatives.** Shards own their nodes exclusively — the shape
Tensorlake describes, where each shard owns a bucket of hosts. Ownership
removes cross-shard collisions entirely and makes the CAS nearly
redundant, which is a much stronger performance story.

**Trade-off.** We keep paying for optimistic concurrency — epoch sampling,
CAS at apply, conflict retries — even in the sharded case where most of it
is provably unnecessary. That is measurable throughput we are leaving on
the table.

**Why (correctness, and reversibility).** Ownership makes shard assignment
load-bearing for the at-most-one-live-allocation invariant: a rebalance
bug, a stale assignment, or a split-brain during leader change could
double-place a node. With shards as pure contention avoidance, the worst a
sharding bug can do is waste CPU or pack badly — the CAS still refuses the
double-place. It also means we can turn sharding on, off, or halfway on
(work stealing) as a config change, and that every existing correctness
test remains valid unmodified. Given that the *measured* problem is
contention rather than a missing invariant, buying safety with performance
we have not yet needed is the right side of the trade.

## 3. In-process shards now; cross-manager planners stay deferred

**Call.** This plan multiplies scheduling inside the leader process. D2
step 2 — followers planning against replicated read models and shipping
plan submissions to the leader — remains deferred and still gated on
AWS-test data.

**Alternatives.** Go straight to cross-manager. It is the architecture
that actually multiplies scheduler CPU by manager count, it is the C1M
shape, and the epoch design was explicitly built so the door stays open —
doing the cheap thing first risks building shard machinery we then have to
rework for the distributed case.

**Trade-off.** The in-process ceiling is one machine's cores. If the AWS
data says we need cross-manager anyway, some of this plan's work
(assignment, rebalance) gets revisited in a distributed context where it
is harder.

**Why (simplicity, and the review's own call).** The architecture review
already made this call for the whole subsystem — keep the single scheduler
until the cheap levers stop paying — and the cheap levers have not stopped
paying: D5 returned only 16–55% because contention, not commit rate, is
the ceiling, and contention is exactly what in-process sharding attacks.
Cross-manager adds a wire surface, follower staleness handling, and real
distributed-debugging burden to a product whose headline claim is
operational simplicity. Doing it before exhausting the in-process win
would be paying that cost on speculation. The shard-assignment work is
also the natural substrate for the distributed version, so it is
groundwork rather than throwaway.

## 4. Sharding does not fix within-shard collisions, and the plan says so

**Call.** The plan states plainly that the measured pathology (4 planners,
same argmax node, 1.25+ retries/plan on a 12-node cell) would be **one
shard** under any scheme here, and therefore that sharding would not have
moved that measurement. Late binding is named as the fix, with a lean that
it lands first.

**Alternatives.** Present sharding as the answer to the contention
ceiling — it is adjacent enough to the measurement to be defensible in a
plan document, and it makes for a much cleaner narrative than "our
headline benchmark cell is immune to this feature".

**Trade-off.** The plan reads as less compelling, and Phase 0 is explicitly
allowed to kill it. We are also sequencing behind another plan we do not
control the timing of.

**Why (honesty is load-bearing here).** The one hard number we have about
contention comes from a cell sharding cannot help. Writing the plan as
though it does would produce a Phase 4 where we measure a win that came
from late binding and attribute it to shards — which is the same
attribution error this whole line of work started by diagnosing in someone
else's blog post. Naming it up front is what makes Phase 0's kill switch
real rather than ceremonial.

## 5. Unbindable evals go to a global shard rather than being broadcast or split

**Call.** An eval whose job binds no pool gets `shard_id = NULL` and runs
against the full fleet, exactly as today. Any worker may claim global work
when its own shard is empty.

**Alternatives.** (a) Assign unbindable evals to a shard anyway (say, by
job-id hash) and let them plan against that shard's slice — every eval
then gets the sharding benefit. (b) Broadcast to all shards and let the
first to plan win.

**Trade-off.** A cluster whose jobs bind no pool puts everything in the
global shard and gets nothing from this plan. That is a real and possibly
common case, and it is why open question 4 exists.

**Why (no surprises).** (a) silently narrows the candidate set for a job
that asked for the whole cluster — the job would be placed on a slice of
the fleet without anything in the jobspec saying so, and the placement
equivalence property (quiet cluster, sharding on == sharding off) would
fail by design. (b) burns N planners to place one thing. The global shard
preserves today's exact semantics for work we cannot partition, which
keeps the equivalence property total rather than caveated — and a
limitation that is stated in the public doc is better than a semantics
change nobody asked for.

## 6. The measurement commits to four series, not a headline

**Call.** Any published throughput number carries
`placements_committed_per_sec`, `conflicts_per_plan`, `correctness`, and
`packing_quality` together, plus a scaling curve rather than a single
cell.

**Alternatives.** Publish the aggregate placement rate with the
methodology in a linked appendix — normal practice, and what the claim
that prompted this work does.

**Trade-off.** A four-column headline is a worse headline, and
`packing_quality` in particular invites the reader to notice that sharding
packs slightly worse. We are volunteering our own weakness.

**Why (the public claim).** A placement rate without a conflict rate can
be produced by a thrashing system, and a sharded placement rate without a
packing number hides what sharding costs. The benchmark README already
holds this line internally; the reason to write it into the plan is that
the pressure to drop columns arrives at publication time, not at design
time. Tying our own hands here is cheap now and expensive later.
