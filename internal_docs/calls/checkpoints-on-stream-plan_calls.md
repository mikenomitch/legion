# Judgement calls: checkpoint work off the heartbeat

Calls made building [checkpoints-on-stream-plan.md](../plans/checkpoints-on-stream-plan.md).
Ranked by project impact, highest first: the number is the rank.

## 1. Requests are pushed as hints on the existing stream, and the heartbeat stays the record

**Call.** A snapshot row that becomes work for its node is pushed in
`StreamWorkReply.checkpoint_requests` on the node's existing `StreamWork`
stream. The push is outside the sequenced set: it goes once per stream, is
stripped from the replay copy, and is not in the digest. The heartbeat reply
keeps carrying the node's whole outstanding set every beat, and that set is
still what tells a worker the manager is done with a piece of work.

**Alternatives.** (a) Make checkpoint requests sequenced entries with the
same replay and digest guarantees placements have, so the stream alone is
authoritative and the heartbeat stops carrying them. That is the symmetric
design, and what the TODO item suggested. (b) A bare "beat now" flag on the
stream, so the worker heartbeats and picks the requests up from the reply,
changing nothing else.

**Trade-off.** Two paths deliver the same request, so the worker needs one
more rule (call 4) to stay correct when they race. A push lost with its
stream costs a heartbeat interval rather than nothing, because nothing
replays it. Against (b) it costs a little more code on both sides.

**Why (simplicity, then performance).** Checkpoint work is derived from rows
the manager re-reads every beat at the cost of one indexed lookup, so a
second authoritative copy buys nothing but a second thing to keep
consistent. Making it sequenced would also mean holding part keys in the
in-memory replay buffer, and replaying requests minutes later on a
reconnect, when they may be long finished. (b) is the simplest of all, but
it costs an extra heartbeat round trip on every request and runs a whole
heartbeat tick (datapath, identity, templates) before the capture can
start. The push starts the work from the stream task, the same way a
placement's launch skips the reconcile.

## 2. Reports go out on an early heartbeat, not a new RPC or a stream frame

**Call.** When a worker queues a fresh checkpoint report it wakes its
heartbeat loop, and the report goes out on an ordinary heartbeat sent at
once.

**Alternatives.** (a) A dedicated worker-plane `ReportCheckpoints` RPC,
Nomad's `Node.UpdateAlloc` shape. (b) A report frame on the stream's
upstream half, next to the acks.

**Trade-off.** An early beat does everything a beat does: the manager runs
the whole handler (liveness, failed allocations, feeds, the reply), and the
worker runs its whole tick. Measured on a streaming node that is about
0.5 ms of manager time (debug build, `docs/architecture-managers-workers.md`),
and each lifecycle adds two (calls 3 and 5).

**Why (simplicity).** The heartbeat is where a report's neighbors already
travel, under one lock: a suspend's stopped allocation, a failed restore's
allocation failure (which the manager must see after the restore report, so
it does not reschedule onto the same checkpoint), terminations, and health.
A separate path would race those and need its own ordering rules, its own
redrive on failure, an ACL classification, and an entry in
`tests/integration/acl_endpoints.rs`. Waking the heartbeat gets ordering, redrive, and
auth for free, and it is the mechanism the stream already uses to run a
reconcile early. Plan Phase 2 keeps (b) as the fallback if early beats ever
show up as load.

## 3. No knob and no coalescing window for early beats

**Call.** Early beats are always on, with no config and no minimum gap.
Wakes that arrive while a beat is in flight fold into the next beat, and a
delete's confirmation never wakes (call 5).

**Alternatives.** (a) A coalescing window, as Nomad's client batches
allocation updates over a short interval before sending them. (b) A worker
or manager setting to turn early reports off for fleets where heartbeat
throughput is the constraint.

**Trade-off.** A node that finishes many captures one after another sends
a beat per capture, and each beat's reports become their own
scheduler-lane group commit unless other nodes' reports are queued with
them. A window would batch per node at the cost of its length in latency.
An operator whose manager is heartbeat-bound has no switch to turn the
extra beats off.

**Why (simplicity, performance).** The extra rate is bounded by the
cluster's checkpoint rate, which the scheduler lane bounds: about 50
suspend-and-resume lifecycles a second on the reference host, so about 100
early beats a second at the ceiling, next to `N / I` scheduled beats
(`docs/heartbeat-cadence-and-scale.md`). The lane's group commit already
batches whatever is queued when an entry finishes, so batching moves from
"per node per beat" to "across nodes per entry" under load, which is where
it matters, while an idle cluster pays no window. The measured ladder in
the plan (§9) is the check: entries per session and the ceiling did not get
worse. A knob that only trades latency back for beats would be one more
thing to tune for no measured gain, so it waits for evidence.

## 4. A worker forgets finished work only after a heartbeat delivered its report

**Call.** `CheckpointState` records, per finished piece of work, whether a
heartbeat the manager answered carried its report (`mark_delivered`). A
reply that no longer lists the request makes the worker forget it only
then. Pushes never forget anything.

**Alternatives.** (a) Keep today's rule (forget whatever a reply does not
list) and accept the race. (b) Tombstones: remember forgotten work for a
while and ignore pushes for it.

**Trade-off.** One more flag per finished piece of work, and finished work
the manager has not heard about is remembered until a beat carries it,
which on a dead connection is until the connection comes back.

**Why (correctness).** With two delivery paths, a reply built before a
snapshot row existed can be processed after the pushed work finished. Under
(a) the worker would forget the report, and if the manager then re-asks
because the report was lost, the worker redoes the work. A redone suspend
capture finds its allocation gone and reports a failure for a suspend that
succeeded. (b) guards the opposite race (a stale push after a correct
forget) and not this one. The delivered rule is exact because the manager
applies a beat's reports before it builds that beat's reply. The opposite
race (a push that arrives after its work was correctly forgotten) needs a
push delayed across a whole report round trip, and at worst redoes work
whose report the manager then ignores, so it is left alone.

## 5. A delete's confirmation rides the scheduled beat

**Call.** Every fresh checkpoint report wakes the heartbeat except a
delete's.

**Alternatives.** Wake for every report, for one rule with no exception.

**Trade-off.** A `deleting` snapshot row lives up to one interval longer
after the node reclaimed its bytes. The reclaim itself still starts at
once, because the delete request is pushed.

**Why (performance).** Nothing waits on a delete's confirmation: no CLI
`--wait`, no instance state, no placement. Every resume deletes its
suspend checkpoint, so waking for it would add a third early beat to every
lifecycle for no visible gain.

## 6. The projection notices snapshot rows, not the RPC handlers that create work

**Call.** The store's changeset projection, which already feeds the work
log on every replica, notes the node of every inserted or updated snapshot
row (re-reading the row by key when an update record does not name its
node). The stream's materializer then reads the node's outstanding work.

**Alternatives.** Call the work log from each place that creates checkpoint
work: SuspendInstance, CreateSnapshot, ForkInstance, DeleteSnapshot, the
TTL sweep, the capture report that makes a snapshot publish work.

**Trade-off.** Any snapshot row update marks its node, including ones that
create no work (a manifest rewrite, a publish error), and each mark costs
the stream one indexed lookup that may find nothing new to push.

**Why (simplicity, correctness).** A worker may stream from a follower,
which never runs the RPC handler, only the projection of the committed
entry. One choke point on every replica cannot miss a new caller, and it
is the same shape the allocation half already uses. Warms are the
exception, since their queue is memory, so WarmSnapshot notes its nodes
directly.

## 7. A publish retried on a timer is not pushed

**Call.** A failed publish becomes work again when its `publish_retry_at`
passes. No row changes at that moment, so nothing is pushed, and the node
picks it up on its next heartbeat.

**Alternatives.** A timer on the manager that marks nodes when a retry
falls due.

**Trade-off.** A retried publish starts up to one interval late.

**Why (simplicity).** A retry is already backing off for seconds or more,
so an interval of extra wait is noise, and a timer wheel for it would be
the only time-driven input to the work log.
