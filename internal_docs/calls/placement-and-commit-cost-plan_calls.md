# Judgement calls: Placement Quality and Commit Cost

Plan: [`plans/placement-and-commit-cost-plan.md`](../plans/placement-and-commit-cost-plan.md).
Covers architecture-review findings SCH2, SCH3, SCH4, SCH5, SCH8, and AI7.

Entries are ranked by project impact, highest first; an entry's number is its
rank.

## 1. Built-in scoring is resource utilization after placing, absolute, and the anti-affinity penalty is scaled by the task's count

**Call.** `bin_pack` scores a node by the dominant fraction of the resources
the task requests that would be in use after placing it (`max_d (used_d +
request_d) / capacity_d` over memory, cpu, cores, and allocation slots with a
finite capacity); `spread` scores one minus that. A small balanced-allocation
term (`0.05 × (1 − (max_d − min_d))`) breaks near-ties toward nodes whose
dimensions fill evenly. Fractions are absolute: a node's score does not
depend on which other nodes are candidates. The automatic anti-affinity
penalty, previously 0.2 per same-task allocation on the node, is now Nomad's
job anti-affinity shape: `0.2 × (collisions + 1) / desired count` when the
node holds any, 0 when it holds none.

```nickel
# unchanged surface; the same keys mean count-scaled now
schedulerSettings = { weights = { bin_pack = 1.0, automatic_anti_affinity = 0.2 } }
```

**Alternatives.** (a) Kubernetes `MostAllocated`/`LeastAllocated`: the
*mean* of per-resource fractions rather than the max. (b) Nomad BestFit v3,
`20 − (10^freeCpu + 10^freeMem)`, which rewards nearly-full nodes
super-linearly. (c) Keep min-max normalization, but of utilization instead of
counts. For the penalty: (d) keep the flat 0.2 per collision; (e) a separate
spreading rule (`maxSkew`-style) instead of a soft penalty.

**Trade-off.** With absolute fractions a small task moves a node's score by a
few hundredths, where the old min-max term always spanned 0–1. The first cut
kept the flat penalty, which then outweighed packing for every task: under
`bin_pack` all replicas spread, plans touched more nodes, and the apply paid
a node-row recheck per extra node — batch-burst 75.2 → 69.0 evals/s and
three other many-replica SLO scenarios 8–15% slower (plan §12). Scaling the
penalty by the count restores packing where it matters: a twenty-replica
task pays 0.2 × 2/20 per first collision, less than one replica's share of a
node, so `bin_pack` fills a node with it (batch-burst 71.5 → 105.4 evals/s
against base, five interleaved rounds). The cost moves to small counts: a
two- or three-replica service still spreads under `bin_pack` (0.2 × 2/3 for
three replicas), as Nomad's does, where count-based scoring stacked it; for
two replicas the penalty is exactly the old 0.2, and high-fanout (ten
two-replica tasks per job) stays within noise of base (132.8 → 126.9) with a
higher apply time from touching more nodes. A job that wants stacking at any
count sets `automatic_anti_affinity = 0`.

**Why (performance/scale and simplicity).** The max is what decides whether
the *next* large task fits — a node with 90% of its memory used and 10% of
its CPU is full for a memory-heavy task, which the mean (50%) hides. Absolute
fractions make the score independent of index truncation, `nodes_considered`,
and `score_sample`, which is what made the review call the old score unsound
on large fleets. Count-scaling the penalty keeps one knob with one meaning
("how strongly to avoid stacking one task") while making its strength
proportionate to the task, which is the property the flat penalty lacked once
the packing term stopped being inflated by min-max normalization.

## 2. Node rows in the apply walk are still parsed from the row, not borrowed from the read model

> **Superseded 2026-11-12** by the follow-up at the end of this entry: the
> walk now borrows the read model's node when the row's bytes match it
> exactly. The original reasoning is kept below because it is why the
> follow-up compares bytes instead of trusting an epoch.

**Call.** SCH5's second half — "read nodes from the read model's
`Arc<Node>`, guarded by the node epoch" — is not shipped. The walk still
SELECTs and parses each attempted candidate's node row once per plan.

**Alternatives.** (a) As the review proposed: use the read model's
`Arc<Node>` when `nodes_epoch` is unchanged. (b) A process-wide parse memo
keyed by a 128-bit keyed digest of the row's attribute columns, verified
inside the transaction. (c) Verify the read model's node by hashing its
re-serialized attributes against the row's raw bytes.

**Trade-off.** The per-candidate parse stays inside the serialized commit:
one parse per distinct node a plan attempts, which the per-plan memo already
bounds. The deferred item is in TODO.md with the design below.

**Why (correctness over performance).** (a) is not sound. `nodes_epoch` is a
process-local counter of read-model mutations, not a version of the row the
transaction sees: the read model is patched *after* a commit, by the
changeset projection, so between a cordon's or a re-registration's commit and
its projection the read model says "available, 64 GiB" while the row says
"draining, 16 GiB". Closing exactly that window is why the walk re-reads the
row (the 2026-07-28 adversarial pass). (b) is sound but duplicates every
node's parsed attributes in a second map (hundreds of MB at 50k nodes). (c)
is sound modulo a keyed-hash collision and reuses the read model's memory,
and is the design recorded in the TODO; it was cut for time in favor of
finishing SCH4 and AI7, and because its gain (serialize+hash vs parse) has
not been measured.

**Follow-up (2026-11-12): compare the row's bytes, not a digest.** The walk
still reads every attempted node row inside the transaction, but borrows the
read model's `Arc<Node>` when the row's eight columns are byte-for-byte what
that node serializes to (`node_row_matches`, a streaming compare that stops
at the first differing byte), and parses the row otherwise. That is
alternative (c) with the hash removed: equal bytes parse to an equal value,
so there is no collision to reason about, and a mismatch costs less than a
parse because it stops early. Measured on `plan_apply_node_rows` (an
eight-placement plan, 10k nodes with ~3 KiB attribute bags): apply 2,404 →
1,887 µs (plan §12.1). The read model is read with `try_read`, so the walk never
waits on a projection while it holds the write connection; a busy lock
falls back to the parse. `last_heartbeat` is not compared because the walk
never reads it. `legion_scheduler_plan_node_rows_total{source}` makes a
workload where rows and model disagree in form (a worker writing
non-canonical JSON) visible, since every apply would then pay both costs.

## 3. `plan_submission` keeps rejections and staged plans in an id window; applied plans write nothing

**Call.** The fused submit-and-apply op (every production plan) writes no
row unless the plan is rejected. The staged two-step API keeps its `queued`
row and the apply that consumes it deletes it. Every insert trims rows more
than 1,000 ids below it (at most 64 per insert); the scheduler-lane leader
sweeps the window plus `queued` rows older than ten minutes on startup and
on leadership.

**Alternatives.** (a) Keep writing every plan and delete it at the end of
the same apply (the review's first option, literally). (b) Keep rejected
plans out of replicated state: an in-memory ring on the leader. (c) A
time-based retention (`created_at < now − 1h`) instead of an id window.

**Trade-off.** The diagnostic query in diagnosing-scheduler-performance.md
now sees the last ~1,000 rejections, not all of them; an incident with more
rejections than that loses the oldest. A staged plan that waits through
1,000 newer records before its apply is gone, and its apply errors ("no
longer exists") — acceptable because nothing in the manager stages plans;
only tests and benchmarks do.

**Why (performance/scale).** (a) still pays an INSERT and DELETE inside
the commit, and both ride the raft changeset of every plan, which is most of
what the review measured. (b) loses the record on failover, exactly when an
operator classifies rejections, and the rejection already commits a
replicated op (its event), so one more row there is nearly free. (c) needs a
time-ordered index and a clock comparison on every insert; the id window is
a rowid range seek that deletes zero or one row in steady state, and it
bounds the table by construction rather than by traffic.

## 4. Gang topology is one attribute key with `required`/`preferred`, defaulting to `required`

**Call.** `gang.topology = { key = "rack", mode = "required" }`. `key` is a
plain node attribute name, looked up like `count.per` and `distinct`.
`mode` defaults to `required`.

```nickel
gang = { min = 8, topology = { key = "nvlink_domain" } },          # required
gang = { topology = { key = "rack", mode = "preferred" } },
```

**Alternatives.** (a) The coordinator's example spelling,
`key = "${node.attr.rack}"`. (b) A multi-level list, Kueue-style
(`keys = ["rack", "zone"]`: one rack, else one zone). (c) `mode` with no
default (required to be spelled out).

**Trade-off.** One level only: a gang that cannot fit in one rack under
`preferred` falls back to the fewest racks that can hold it, with no
preference for those racks sharing a zone. `${node.attr.…}` interpolation
does not exist anywhere else in a jobspec, so (a) would introduce a second
way to name an attribute.

**Why (simplicity).** Every other place a jobspec names a node attribute
(`count.per`, `distinct`, constraints) uses the bare key, so the topology key
reads the same way. `required` is the default because a gang author who
names a domain almost always means it (NVLink and rack locality are
throughput cliffs, not preferences), and a default that silently spans
domains would hide the cliff until the training run is slow.

## 5. Topology placement tries whole domains, best fit first, at most eight per plan

**Call.** For a topology gang the planner estimates how many replicas fit in
each domain (free capacity over the request, per node, summed), then runs a
full placement inside one domain at a time — domains that fit every replica,
smallest estimate first, then domains that fit the quorum, largest first —
rolling back a failed try. At most eight domains are tried per gang per
plan. A top-up of a running gang is confined (`required`) or steered first
(`preferred`) to the domain holding most of its running members.

**Alternatives.** (a) A scoring term: add a bonus for nodes in the domain the
gang's first member landed in. (b) Try every domain. (c) Pick the domain
with the most free capacity (worst fit).

**Trade-off.** Eight tries bounds planning cost on a cluster with hundreds
of domains, at the price that a gang whose fit depends on preemption may not
try the domain where preemption would have made room. The estimate ignores
anti-affinity, ports, and `distinct`, so a domain can estimate as fitting and
then fail its try (it is then rolled back and the next is tried).

**Why (simplicity and correctness).** A scoring bonus cannot express
`required`: the first member's domain is chosen before anyone knows whether
the rest fit there. Trying domains whole gives the gang's all-or-nothing
rule a domain-sized unit, and best fit (Kueue's default) keeps large domains
whole for large gangs, the same argument as SCH4's `bin_pack`.

## 6. The claim's pending count is capped at the fair share's saturation point; the queue-depth gauge is rate-limited

**Call.** A claim counts pending rows only up to `workers × max + held`
(what `share_of` can use). `legion_eval_queue_depth` is set exactly from
that count when the queue is shallower, from a full count at most once a
second when deeper, and incremented by enqueue.

**Alternatives.** (a) An exact in-memory pending counter maintained on
enqueue, settle, delete, cascade, prune, and raft apply. (b) An in-memory
eval broker (Nomad).

**Trade-off.** The gauge can lag by up to a second on a deep queue, and an
enqueue racing another can lose a `+1` until the next claim corrects it.

**Why (simplicity).** Every path that deletes eval rows (settles, cascades,
job purges, the stale-eval prune, changeset apply on followers) would have to
keep (a) honest, and a drifting counter is worse than a slightly stale one.
The batch size needs only "at least the cap"; the gauge needs only to be
roughly current.

## 7. The claim queries steer SQLite with `+ready_at` instead of dropping the `(status, ready_at, …)` index

**Call.** The head and duplicate queries write `+ready_at <= ?` so SQLite
cannot range on `ready_at`; two new partial indexes over pending rows serve
the duplicate lookup and the non-planning claim. Query plans are pinned by a
unit test.

**Alternatives.** (a) Drop `idx_eval_status_ready_priority_id`, which
`next_pending_eval_delay` uses. (b) `INDEXED BY`. (c) Run `ANALYZE`.

**Trade-off.** The head now walks `priority DESC, id` and filters
`ready_at` per row, so a queue whose head is many *delayed* evals walks past
them; that is the same O(n) as before, only in the rare shape.

**Why (performance).** Without statistics SQLite preferred the `ready_at`
range and sorted every ready row (2.8 ms at 50k in a schema copy). `+col` is
the documented, local way to take one term out of index selection; (b) fails
the statement if the index is missing, and (c) would make plans depend on
data and on when `ANALYZE` last ran.

## 8. Spot-interruption awareness for gangs is deferred

**Call.** No interruption handling ships. Documented as a limitation; TODO
entry added.

**Alternatives.** A node condition ("interruption pending") set from IMDS by
the worker or from the ASG lifecycle hook by the `aws-asg` plugin, which
drains the node and restarts its gangs ahead of the loss, and keeps fresh
gangs off it.

**Trade-off.** A spot reclaim is handled as a node loss after the fact: the
gang's `onMemberLoss` (default `restart`) restarts it whole, two minutes
later than it could have.

**Why (scope).** It needs a new node condition, a source on two sides
(worker and plugin), and a checkpoint story; none exists, and a half-built
version would be a jobspec promise without a mechanism. The existing
member-loss path already gives the correct end state.

## 9. Per-plan and per-claim logs move to `debug`; one `info` line for a plan slower than a second

**Call.** As SCH8 asked, plus a slow-plan `info` line (≥ 1 s).

**Alternatives.** Sampling (every Nth plan at info); no info line at all.

**Trade-off.** An operator who relied on the info lines to watch throughput
reads metrics instead (`legion_eval_phase_seconds`, `legion_plan_apply_total`).

**Why (simplicity).** AGENTS.md's tracing guidance; the slow-plan line keeps
the one event a person would want to see without turning on debug.
