# Judgement Calls: Sandbox Scaling to Very Large Session Counts

Decisions made while writing
[`plans/sandbox-scaling-plan.md`](../plans/sandbox-scaling-plan.md), and
while implementing its Phases 0–3 (entries 5, 10, 11, 13, 14, 16, 17, 22,
and 28–40). **Entries are ranked by project impact, highest first. The
number is the rank.** Implementation-time calls get inserted at their rank
as the phases execute. If a citation elsewhere names an entry by number,
fix it when you renumber. The plan cites entries by number throughout.

The exec entries that started here moved to
[`sandbox-exec-plan_calls.md`](sandbox-exec-plan_calls.md) with the exec
work.

## 1. Running sessions are node-owned soft state, and raft keeps only what must be durable

**Call.** The recommended architecture combines brainstorm options 1, 2,
and 3. A running session in `session` mode is recorded on its node, in a
local log, and in a leader-held index rebuilt from node reports. An idle
session is a manifest in the content store. Raft holds jobs, envelopes,
and promoted (named or durable) instances. A session's start, suspend,
resume, and stop write nothing to raft.

**Alternatives.**
- (a) Stop at group commit and write trimming (option 5). It is the
  cheapest change and keeps every guarantee. It is Phase 1 either way.
- (b) Write-behind for keyed instances: persist the record asynchronously
  and let nodes re-report after a crash. It keeps one instance model.
- (c) Shard instance rows across raft groups first (option 4), keeping
  exact semantics everywhere.
- (d) An external registry (option 6).

**Trade-off.** Unnamed sessions get weaker guarantees than keyed
instances: probabilistic id uniqueness, converge-to-one instead of
at-most-one, an eventually consistent list, and loss on node failure while
running (plan §6.7). Platforms have to know which mode they are in. The
code gains a second source of truth for "what is running", scoped to one
mode.

**Why (performance / scale, then simplicity).** (a) still costs raft
entries and rows per session, so it raises the ceiling by a constant and
leaves the resident-count wall (tombstones, in-memory snapshots) where it
is. (b) makes keyed instances lie about guarantees they document, which is
worse than a separate mode that states its own. (c) multiplies operational
surface (N groups, scatter-gather, cross-cell moves) to keep per-session
records nobody needs to be exact. Every system in the prior art past about
10^5 resident units (Borg, flyd, Modal, Lambda slots) made the host
authoritative. Legion already did the same for liveness, the location
index, and warms, so this extends a pattern operators already run.

## 2. Sessions are a third instance mode, not a new noun

**Call.** `stateful.mode = "session"` beside `slot` and `keyed`. The
existing `legion instance` commands, RPCs, and MCP tools work on sessions,
and replies carry `durable` and, for lists, `complete`.

```console
$ legion instance create agents -j
{"ok":true,"data":{"instanceId":"workspace-7k2qd9x4m1b8c3rj5t0v6wye2h",
 "mode":"session","durable":false, ...}}
```

**Alternative.** A separate `legion session start|list|get|suspend|resume`
surface and `Session` RPCs, which makes the different guarantees
impossible to miss.

**Trade-off.** A caller can read `instance list` and assume exactness when
some rows are best effort. The `durable` and `complete` fields carry that
distinction, and an agent that ignores them can be wrong.

**Why (simplicity).** Promotion turns a session into a keyed instance in
place. With two nouns, promotion would move an object between APIs and
every consumer (UI, MCP, catalog, ACL tests) would handle both. One noun
keeps one set of tools for agents and one ACL vocabulary.

## 3. Naming promotes, and unnamed ids are 130 random bits with no tombstone

**Call.** A caller-chosen id always makes a durable keyed instance. A
server-minted id is `{task}-{26 base32}`, 130 random bits, never checked
and never tombstoned, and caller ids may not take that shape. Phase 4
applies this to server-minted keyed ids too, so it changes an existing
documented guarantee from "never recycled, by record" to "never recycled,
by randomness".

**Alternatives.**
- (a) Keep tombstones for every id, as today.
- (b) Make untombstoned server ids opt-in per job.
- (c) Let sessions take caller names without promotion, checked against
  the soft index.

**Trade-off.** At a trillion ids the collision chance is about 10^-15, not
zero. A `subTemplate` with `{instance}` puts that probabilistic id inside
a security boundary. Phase 4 needs an upgrade note because it changes a
guarantee existing users read.

**Why (scale, then simplicity).** (a) makes raft size grow with history,
which is the resident-count wall. (b) adds a knob whose right answer is
the same for almost everyone. (c) cannot work, because a soft index can
answer "is this name taken" wrongly during a rebuild, and a name is
exactly what a platform keys its own data on. Cloudflare's split between
`newUniqueId` and `idFromName` is the precedent: random ids skip the
uniqueness check, and names pay it once. The reserved shape closes the
remaining hole, a caller typing in a dead unnamed id, with the same kind
of rule slot ids already have.

## 4. Work reaches workers as a sequenced delta stream held in leader memory, with the full set as resync

**Call.** Phase 2 replaces "best-effort push plus the full desired set on
every heartbeat" with a per-node delta log in the leader's memory, built
from the changesets it already applies. Each delta carries `(leader_epoch,
seq)`. The worker holds `Manager.StreamWork`, acks its last applied `seq`,
and gets today's full desired set once on failover, regression, or a gap.

**Alternatives.**
- (a) Tensorlake's outbox: write each command durably in the placement
  transaction, so delivery survives a leader change without a resync.
- (b) Keep the full set and add a desired-state version, so an unchanged
  node gets a cheap "unchanged" reply (the open TODO).
- (c) Make the push reliable (retry until acked) and keep the full set as
  is.

**Trade-off.** A leader failover makes every worker resync once, which on
a 10,000-node fleet is a burst of full sets. Two delivery paths exist
during the version-skew window. The stream is one more long-lived
connection per node on the leader.

**Why (performance, then simplicity).** (a) adds rows to every placement
entry, and every ack and prune becomes a raft write per node per beat, the
cost Phase 1 removes from allocation health. Legion's allocation rows are
already the durable record, so a durable outbox would store everything
twice. (b) fixes the steady state but still rebuilds and sends thousands of
payloads on any change, and still leaves a lost push waiting up to a
heartbeat interval (21 s at fleet scale). (c) keeps the per-beat full set.
Sequenced deltas with a full resync give round-trip latency and constant
heartbeat size, and the fallback is the code that runs today.

## 5. The delivery epoch is per manager process, and any manager serves the stream

**Call.** Phase 2 deviates from entry 4 in two places. The epoch a stream
position belongs to is a random value chosen when the manager process
starts, not the raft term the leader took office in. And the delivery log
lives on whichever manager the worker streams from, leader or follower,
fed by the changeset projection every replica already runs, not only in
the leader's memory.

```text
StreamWorkOpen { node_id, last_epoch, last_seq }
  last_epoch == this process's epoch and last_seq still held -> replay
  anything else                                             -> full resync
```

**Alternatives.**
- (a) The plan's shape: the leader holds every stream, the epoch is its
  term, and followers redirect workers to it.
- (b) A replicated per-node sequence, so any manager could continue any
  stream after a failover.

**Trade-off.** A manager restart resyncs its workers even if it was not
the leader, and two managers serving the same node at different times give
it unrelated sequences (the epoch tells them apart, so the cost is a
resync). Each manager pays the projection's per-change lookup for the nodes
streaming from it.

**Why (simplicity, then performance).** Workers already dial one fixed
manager forever and heartbeat to it (liveness plan §6.3); (a) would make
the stream the one RPC that has to find the leader, and a leader failover
would resync every worker in the fleet at once, which is the burst entry 4
accepted. With the log on every replica, a failover disturbs only the
streams attached to the manager that died
(`tests/work_stream.rs::a_leader_kill_resyncs_only_the_workers_that_lost_their_manager`:
the surviving manager's worker saw zero resyncs, the orphaned one exactly
one). The raft term would also not be a valid epoch on a follower, and a
process-random epoch costs nothing. (b) puts a write on every delivery,
the cost entry 4 rejected.

## 6. Idle sessions are manifests, and archiving keyed instances ships before session mode

**Call.** A suspended session is a manifest in
`<contentStore.url>/sessions/<job>/...`, with no raft row. Phase 5 brings
the same idea to keyed instances first (`suspend.archive`), keeping a slim
row for named ones.

**Alternatives.**
- (a) Keep idle instances in raft and shard later.
- (b) Put every idle instance in the bucket, named ones too, and use the
  object store's conditional put as the uniqueness authority.
- (c) Ship session mode first, since that is where the zero-write promise
  lives.

**Trade-off.** Listing idle sessions becomes a paginated bucket listing,
slow at 10^8 objects. An idle session's existence depends on bucket
lifecycle rules Legion does not own. A named archived instance still costs
one slim row.

**Why (scale, then simplicity).** Idle sessions are the majority on an
agent platform, so they set the row count. Published checkpoints already
put the bytes in the bucket, so the manifest adds identity, not a new
storage system. (b) makes S3 conditional writes part of Legion's
correctness story for names, a dependency the `file://` content store
cannot honor. (c) delays the bigger win behind the bigger design: Phase 5
works on today's keyed model and is valuable even if Phase 6 never ships.

## 7. Raft places capacity envelopes, and a router fills them

**Call.** A session-mode task declares `stateful.envelope`, a per-node
reservation that the scheduler places as an ordinary allocation. Sessions
fill envelopes through a manager-side router (first fit, locality-aware)
or edge admission. No raft write per session.

```nickel
envelope = {
  resources = { cpu = 30000, memory = 61440 },
  constraint = [{ attribute = "pool", value = "agents" }],
},
```

**Alternatives.**
- (a) No envelope. The router places sessions anywhere with free capacity
  from live node usage (pure option 2, Modal's shape).
- (b) Raft places pools of warm VMs (option 7 as a VM pool).

**Trade-off.** Envelopes reserve capacity whether or not sessions fill
them, so a cluster mixing sessions and services has to size envelopes.
Spare envelope room is invisible to other jobs except through
oversubscription. The job author writes one more block.

**Why (simplicity).** (a) makes the scheduler blind to most of the
cluster's load, so quotas, preemption, `node usage`, and bin packing for
everything else become wrong. Envelopes keep every existing scheduler
feature correct with zero changes to the scheduler, at the cost of one
allocation per node per pool. (b) ties the envelope to one VM and to one
isolation tier. Borg's alloc and Lambda's slots are the precedent: place
the container of capacity slowly and durably, fill it quickly and softly.

## 8. This plan owns wake on request for idle sessions, at a gateway, not on the node

**Call.** Wake for a session that exists only as a bucket manifest is
built in a stateless gateway (`legion start gateway`, Phase 7). The suspend
plan's Phase 8 keeps node-local wake on connect for local checkpoints, and
the gated plan's Phase 5 keeps the last hop into gated guests.

```nickel
suspend = { durability = "store", wake = { on = "request", port = 8080, timeout = "10s" } },
```

**Alternatives.**
- (a) Extend the suspend plan's parked listener: keep the port reserved on
  the node that suspended the session and resume there.
- (b) Leave waking to platforms, which already call `instance start`.

**Trade-off.** Operators who want wake on request run one more stateless
tier, and requests take one extra hop. Two wake mechanisms exist, node-local
and gateway, chosen by where the checkpoint lives.

**Why (scale).** (a) pins every idle session to a node and its port, which
is the cost manifests exist to remove: an idle session in the bucket has no
node. (b) is the status quo and is what makes every platform build its own
proxy, the way Fly and Tensorlake did. The gateway also hosts the exec
plan's gateway phase, so one tier serves both.

## 9. Unplaceable work blocks under the key that blocked it, and only matching capacity wakes it

**Call.** Phase 3 makes an eval that cannot place `blocked` with a key
(node class, resource shape, checkpoint pin, or `escaped`), indexed in
leader memory, one per job. Node joins, uncordons, freed capacity,
attribute changes, budget reports, and warms wake only matching keys.

```console
$ legion job status agents -j
{"ok":true,"data":{"status":"blocked",
 "blocked":{"key":"class:pool=agents,cpu.arch=x86_64","shortfall":{"memory":"48GiB"}}}}
```

**Alternatives.**
- (a) Keep the cluster-wide capacity pass and add freed capacity as a
  trigger.
- (b) Retry failed evals on a timer with backoff.
- (c) Wait for the gang plan to build its blocked-eval queue and reuse it.

**Trade-off.** Evals that end `failed` today end `blocked`, which is a
visible change of default. Computing node classes adds work at
registration and on attribute changes. The escaped bucket means some jobs
still wake on everything.

**Why (performance, then simplicity).** (a) re-plans every short job in the
cluster on each event, so freed capacity on a dense sandbox fleet would turn
every suspend into a cluster-wide pass. (b) burns evals while nothing
changed and still waits a backoff when something did. (c) puts a generic
need behind a gang-specific plan. Nomad's blocked evals are the proven
shape, with escaped and missed-unblock tracking, and the gang plan becomes
a consumer instead of a second implementation.

## 10. Unplaceable evals end `blocked`, not `failed`, and the job reports it as a read overlay

**Call.** A `job_submit`, `job_update`, `job_reschedule`, or `job_scale`
whose plan finds no room is written `status = 'blocked'` with its key and
detail, in the settle that used to write `failed`. A job with a blocked
eval reports a `blocked { key, task, shortfall, message, since, evalId }`
block from `GetJobInfo` and `ListJobs`, and `status: blocked` where the
derived job status (job-status plan, merged alongside this work) would
read `pending`. A job with live allocations keeps `running`. Nothing is
stored on the job.

```console
$ legion job info agents -j
{"ok":true,"data":{"status":"blocked","blocked":{"key":"class:pool=agents",
 "task":"workspace","shortfall":{"cpu":"500MHz","memory":"3000MiB"}, ...}}}
```

**Alternatives.**
- (a) Store `blocked` on the job row (a control-lane write when an eval
  blocks and another when it wakes).
- (b) A `legion job status` verb, as the plan's console sketch shows.
- (c) A cluster-wide shortfall: the job's total unmet request against the
  class's free capacity.
- (d) Let `blocked` outrank `running` in the derived status, so any job
  with a waiting placement reads `blocked`.

**Trade-off.** `failed` no longer means "no room" for these evals, so a
script that read `eval.failed` as a capacity signal must read
`eval.blocked` or the job's `blocked` block (the upgrade note in the PR
says so). `shortfall` is what one placement asks for, which tells an
operator what kind of node to add but not how many. Rollout ticks and the
node-scoped passes keep their old outcome (entry 31).

**Why (simplicity).** The job row is control-lane state and the eval is
scheduler-lane state; (a) would make every block and wake a cross-lane
composite for a value derivable from one indexed read. There is no `job
status` verb today and `job info` is where a job's state already lives, so
(b) would have added a second command for the same record. (c) needs a
capacity model of the class the scheduler does not keep, and computing it
on every read of `ListJobs` is the cost a list should not pay. (d) would
make a 10,000-instance sandbox job serving traffic read `blocked` because
one create is waiting; the job-status plan's rule that live work reads
`running` is the right first answer, and the block carries the rest.

## 11. Eval claims are leader memory, not a raft entry

**Call.** Phase 1 removes the claim write. `claim_evals` marks evals in
the claiming manager's memory and subsumes their pending duplicates there;
the rows stay `pending`, and the settle deletes the eval and its
duplicates in one write. `get_evals` overlays `in_flight` on the leader.
A manager that steps down or starts forgets its claims.

**Alternatives.**
- (a) Keep `pending → in_flight` durable and fuse the claim into the
  enqueue when a worker is idle.
- (b) Keep the claim durable and rely on batching to amortize it, as
  before.

**Trade-off.** Another manager's `eval list` shows a claimed eval as
`pending`. A failover re-runs the evals the old leader had claimed, as
`requeue_in_flight_evals` did before; there is no durable record of which
evals were mid-flight, so `scheduler.recovered` no longer fires for them.
Two eval workers on one manager still never share an eval (one lock), but
the exclusivity is now process-local, which is only correct because one
manager runs evals at a time.

**Why (performance, then simplicity).** On an idle cluster every placement
paid the claim as an entry of its own: 1 of the 4 the Phase 0 count found
for "place". The only thing the durable claim bought was recovery, and an
unclaimed `pending` row gets the same recovery for free: the next leader
runs it, and handlers are idempotent reconciles, which the old recovery
also relied on. (a) still writes when the queue is idle, which is exactly
when the claim was a round of its own. "Place" went from 4 entries to 1
with this and entry 16 together.

## 12. Measure, then group commit, before any new semantics

**Call.** Phase 0 is a gate that measures entries per lifecycle, rates,
and bytes per row, and leaves an op-count test in CI. Phase 1 trims and
batches writes inside today's model: allocation health only on change,
coalesced creates and snapshot reports, fused events, ratification that
creates its own row and spends its ticket in one apply, and no claim and
settle for evals with nothing to place.

**Alternatives.** Skip to session mode, since only it removes per-session
writes. Or make raft itself faster (pipelining more than one proposal per
lane).

**Trade-off.** Two phases of work that do not change the asymptote. Phase
0 also needs loadgen work (an instance mode, simulated allocation health)
before it produces anything.

**Why (performance).** The static count found writes nobody had measured,
including a per-node, per-beat allocation-health write that grows with
sandboxes per node. Session mode would inherit some of these (envelopes
are allocations with health). Phase 1 helps every keyed user now, without
a new mode, and the eval-path plan already showed that fewer rounds beat
faster rounds on a serial lane.

## 13. Phase 0 was measured on a stated single-host substitute, not the AWS rig

**Call.** Every Phase 0 and later number in this PR comes from one 4-vCPU
Intel Xeon (2.1 GHz, 15 GiB, no KVM), a release build, a single-voter raft
manager (`single` topology), and `legion-loadgen --mode instances` with 50
embedded simulated nodes. Runtime boot and restore are simulated (no KVM),
so the §7.2 runtime rows record the control-plane share only. The op-count
test is exact on any host and runs both topologies.

**Alternatives.** The AWS rig (3 × m6id.xlarge, `lanes`), or skipping the
latency SLOs until the rig runs.

**Trade-off.** Absolute numbers are lower than the rig's and the loadgen,
the fleet, and the manager share four cores; a single voter pays fsync but
no network quorum, so raft latency is understated and the scheduler lane's
serial cost is the bound instead. Results are not comparable with AWS
cells.

**Why (simplicity).** The plan allows "the AWS rig or a stated local
substitute", and this session had no cloud access. The phase targets are
ratios on the same box (≥ 5× rate, ≤ 8 entries), which a single host
measures fairly when every phase runs on it in the same way, and the
entry counts that drive the rest of the plan do not depend on the host.

## 14. The blocked-eval index is the rows, and every replica keeps the capacity-event log

**Call.** Entry 9 put the key → eval index in leader memory, rebuilt from
rows on election. The implementation keeps no second index: the wake pass
reads the `blocked` rows (at most one per job) only when it has capacity
events to match. The events themselves are recorded by the changeset
projection on every manager, numbered, for five minutes. A manager that
takes the scheduler lane replays the last 60 seconds of them; a manager
process that starts as the scheduler's writer, with no log to replay,
returns every blocked eval to `pending` once (cause `startup`).

**Alternatives.**
- (a) The plan's leader-memory index from key to eval ids.
- (b) Replicate the capacity events, so a new leader has an exact log.
- (c) On election, wake every blocked eval (as on startup).

**Trade-off.** A pass reads every blocked row even when only one class
changed, which at 10^4 blocked jobs is a 10^4-row read per pass with
events (passes coalesce bursts to one per 50 ms). A restart costs one
re-plan per blocked job. Capacity that appeared more than 60 seconds
before an election and was never acted on is missed (a leader that lived
past it acted on it).

**Why (simplicity, then performance).** The rows are already replicated
and indexed by status; a memory index is a copy that must be kept
consistent with every write that blocks, replaces, deletes, prunes, or
wakes an eval, on every leader change. Matching is per task anyway (the
key string cannot express `>` or `matches`), so the index would only have
narrowed which rows to test. (b) adds a raft write per stop. (c) turns
every election into a re-plan of every blocked job; the replay window is
exact for the window that matters
(`tests/blocked_evals.rs::a_new_leader_rebuilds_from_rows_and_replays_a_missed_wake`).

## 15. Every phase carries p50 and p99 latency targets, measured first in Phase 0

**Call.** The plan sets latency SLOs (placement decision, dispatch, running
from cold boot and from restore, resume from local memory, published
memory, and disk, wake on request, and exec open) beside its throughput
targets, measured at p50 and p99 on an idle cluster and at 80% of the
throughput ceiling.

**Alternative.** Keep throughput and resident counts as the only targets,
as the scheduler benchmarks have, and treat latency as a side effect.

**Trade-off.** Phase 0 gets bigger, and several targets are set before any
measurement, from published numbers (Modal, Fly, Google, Tensorlake) that
were not taken on Legion's hardware. Some will be rewritten.

**Why (performance).** An agent platform's user feels the start and the
resume, not the cluster's placements per second, and the benchmark history shows the two moving apart: the 2026-08-04 AWS campaign cut plan p99 by
42% while throughput did not move (eval-path-raft-cost plan §1). A
phase that raises throughput by queuing longer would pass a
throughput-only bar.

## 16. An eval settles inside the plan that finishes it

**Call.** A handler whose plan is its last write arms its settle
(`arm_eval_settle`); the plan apply then deletes the eval row, deletes the
duplicates its claim subsumed, and records `eval.completed` in its own
entry. Today only `job_reschedule` without a scheduled batch follow-up
arms it, which is the placement path every keyed create and resume takes.

**Alternatives.**
- (a) A deferred settle piggybacked on the next write the scheduler lane
  makes.
- (b) Keep the separate settle and amortize it by batching.

**Trade-off.** Two code paths settle an eval (the fused one and the batch
settle), and the eval worker must not write a second outcome for a fused
one (`EvalCompletion::Settled`). A handler that arms and then writes again
after its plan would settle too early, so arming is opt-in per handler.

**Why (performance).** At true idle (a), still costs one entry per eval,
only later; (b) amortizes nothing when the queue holds one eval, which is
the steady state for interactive creates. Fused, a placement is one entry.

## 17. Stop and destroy write directly and queue no eval

**Call.** `StopInstance` and `DestroyInstance` remove the allocation and
change the instance row in one group-committed scheduler-lane write, and
enqueue no eval. Capacity they free reaches waiting work through the
blocked-eval wake pass (`freed` events).

**Alternatives.** Keep the eval (destroy, enqueue, claim, settle: the
Phase 0 count's 4 entries) and skip only its claim and settle when the
eval finds nothing to place, as the plan's Phase 1 bullet says.

**Trade-off.** The removal no longer passes through the eval loop, so a
job's other evals are not re-run by a stop; anything that relied on the
stop's eval to re-plan the job (a job short elsewhere) now waits for its
own blocked eval.

**Why (performance).** The eval had no placement work: removing an
instance never places anything. Skipping claim and settle would still have
paid the enqueue; writing directly makes destroy 1 entry instead of 4,
and Phase 3's wake pass already covers the one thing the eval could have
done for other jobs (use the freed room).

## 18. Unnamed sessions converge to one, and idempotency derives the id

**Call.** Two nodes may briefly run the same unnamed session id. The
leader stops the one with the later lease within a heartbeat. A create
with an `idempotency_key` derives its id as an HMAC of the accessor, job,
and key, so a retry lands on the same id without a raft record.

**Alternatives.** Record idempotency keys in raft for sessions too. Or
route every create for a key through the leader to dedupe in memory.

**Trade-off.** A retry that races to two nodes boots twice for up to one
heartbeat interval, and a side effect from the loser is possible. The
at-most-one guarantee is only for promoted sessions.

**Why (scale).** A raft idempotency record per create is a raft write per
session, which is what the mode exists to avoid. Leader dedupe works but
makes the leader a per-create hop. The derived id gives the agent the one
property it needs, the same id on retry, and a duplicate is rare and
bounded.

## 19. No external session registry, not even as a seam

**Call.** Option 6 is rejected as a default and is not built as a plugin
seam in this plan.

**Alternative.** A `SessionRegistry` plugin with DynamoDB, FoundationDB,
or Spanner backends, keeping raft as the control plane only. GKE's move to
a Spanner-backed store is the strongest argument for it.

**Trade-off.** A platform that already runs FoundationDB cannot point
Legion at it. If the node-owned design fails at some scale, there is no
escape hatch short of new work.

**Why (simplicity, extensibility deliberately closed).** Legion's pitch is
one binary. A registry backend makes correctness depend on a system the
operator must run or rent, and a seam built before a second implementation
exists usually fits the first one only. The node-owned design needs no
external store beyond the content store that published checkpoints already
require.

## 20. Cells are deferred behind a measured trigger

**Call.** Key-range sharding of instance rows across raft groups is Phase
7, built only when Phase 0's harness shows the scheduler lane above 70%
busy from instance writes, or instance rows above 4 GiB of store.

**Alternative.** Build cells now, as the general answer to raft limits,
on top of the multi-raft lanes that already run several groups.

**Trade-off.** If promoted instances grow faster than expected, cells
arrive late. The thresholds are guesses until Phase 0 replaces them.

**Why (simplicity).** After Phases 4 to 6, raft holds envelopes and named
instances, which grow with users who name things, not with sessions.
Cells add scatter-gather lists, cross-cell renames, and per-cell
leadership to every operator's mental model. That cost is worth paying
only with evidence.

## 21. Prefix tickets stand in for leases at the edge, and spend once

**Call.** An edge-started session uses a multi-use, instance-prefix boot
ticket as its authorization. The node mints the session id. The ticket is
spent per ticket, not per session.

**Alternative.** Keep one single-use ticket per session, so every session
has a manager-recorded capability.

**Trade-off.** A compromised edge node holding a prefix ticket can start
any number of sessions under that job until the ticket expires. Audit
links sessions to the ticket `jti`, not to a per-session mint.

**Why (scale).** A single-use ticket is a control-lane write to mint and
another to spend, two raft writes per session. Edge boot already allowed
prefix and multi-use tickets and already accepted "runs anything a live
ticket names" as the edge blast radius.

## 22. The ticket spend rides the ratification, and an edge start can be the create

**Call.** `boot_ticket` moves to the scheduler lane. A ratification spends
its ticket, creates the keyed instance when the ticket names an id that has
never existed, places it, and records `provisional.ratified`, all inside
one savepoint of one group-committed entry. A refused ratification
therefore leaves a single-use ticket unspent.

**Alternatives.**
- (a) Keep the spend on the control lane and add a spend coalescer.
- (b) Keep requiring `CreateInstance` before an edge start.

**Trade-off.** A behavior change: before, a refused ratification still
burned the ticket. Under `lanes` the ticket table now follows scheduler
leadership. A tombstoned id is still refused (`instance_claimed`), so
"never existed" means no row at all.

**Why (performance, then simplicity).** (a) leaves two serial commits per
start on two lanes. One entry is what the plan's Phase 1 target asks for
(an edge start is one scheduler-lane entry plus the status projection),
and the double-spend refusal still holds because the spend is a
conditional update inside the same transaction
(`tests/edge_boot_integration.rs::a_ticket_is_spent_only_by_a_ratification_that_commits`).
(b) costs a create and its placement eval per start, the very writes edge
boot was meant to skip.

## 23. Per-session events stay off the replicated journal

**Call.** Session lifecycle events go to the live `StreamEvents`
broadcast and to the node's session log, shipped in batches to the bucket.
Only promotion, envelope changes, and misconfiguration refusals are
journaled.

**Alternative.** Journal every session event, as keyed instances do.

**Trade-off.** `legion events` after the fact does not show a given
unnamed session's history. An investigator reads the shipped log or the
node, which is slower. A node that dies before shipping loses its last
interval of log.

**Why (scale).** The journal keeps about 10,000 rows. At 20,000 session
starts per second it would roll over in under a second, so journaling
costs raft writes and still loses the record. The shipped log is the
honest audit trail at that rate.

## 24. Session workload identity comes from the lease, and the boundary moves to the envelope

**Call.** The manager issues tokens for a session after verifying the
node, the lease (or ticket), and the envelope allocation, not an
allocation row per session. `legion.io/alloc` names the envelope and
`legion.io/instance` the session.

**Alternative.** Promote any session that asks for a workload identity,
so tokens always come from a row.

**Trade-off.** A compromised node with a prefix ticket can mint identity
tokens for session ids it invents under that job. Today it can mint them
only for allocations placed on it. The default `sub` has no instance, so
federation policies keyed on `sub` see no change, but per-instance
policies on unnamed sessions are weaker.

**Why (scale, with the security cost stated).** Most agent sandboxes need
a credential, so promote-on-identity would put nearly every session back
in raft. The blast radius matches what edge boot already accepted, and
promoted sessions keep today's boundary for workloads that need it.

## 25. The target is millions running and hundreds of millions idle per cluster, not billions running

**Call.** The plan's per-cluster targets are at least 10^6 running
sessions and 10^8 idle ones, with billions reached through federation.

**Alternative.** Set a billion-session target for one cluster to match
the claims that prompted the plan.

**Trade-off.** The plan's headline is less exciting than the claims it
answers.

**Why.** A billion running sessions at about 10 MiB is about 10 PB of
RAM. The research found no primary source for "billions of concurrent
agents". The largest primary claims are Cloudflare's tens of millions of
agents and Google's tens to hundreds of millions of instances, mostly
idle. A target the design can be measured against beats one it cannot.

## 26. Snapshot-clone density before a sub-VM tier

**Call.** Phase 8 densifies with one microVM per session sharing pages
from a warm base checkpoint (option 8). Phase 9 adds only a seam for
many sessions per VM (option 7), with no runtime.

**Alternative.** Ship a process or WASM tier inside a pooled VM first,
which gives 10 to 100 times more sessions per node.

**Trade-off.** Tier 1 density is bounded by per-VM overhead (about 5 MiB
of VMM plus the guest's private pages), so some workloads that would fit
in an isolate pay for a VM.

**Why (simplicity and security).** Legion's sandbox users run arbitrary
Linux code, and a shared kernel is a different security promise. Tier 1
keeps the promise and reuses fork machinery that already shares pages on
Firecracker. Tier 2 belongs to a runtime plugin that can make its own
isolation claim.

## 27. The worker's session log is a new SQLite database, not the JSON state file

**Call.** Sessions are recorded in `sessions.db` on the worker. The JSON
`WorkerStateStore` keeps allocations only.

**Alternative.** Extend `WorkerStateStore`, which already holds the
restart record.

**Trade-off.** The worker has two local stores to back up and reason
about.

**Why (performance).** `WorkerStateStore::save` rewrites the whole file
on every change (`src/worker/state.rs:151`). At 10,000 sessions per node
with churn that is quadratic I/O. Fly's `flyd` and Nomad's client state
both use an embedded database for this reason.
## 28. A streamed allocation starts through the push path, and everything else through the reconcile

**Call.** The worker starts an allocation that a delta added at once, by
calling its own `PostWork` handler (identity delivery, launch, refreshers,
exactly as a push). Stops, payload changes, and resyncs wake the heartbeat
loop, whose reconcile runs against the stream's mirror instead of a desired
set in the reply. A resync's additions always go through the reconcile.

**Alternatives.**
- (a) Wake the heartbeat for every message and let the reconcile start
  new work too.
- (b) Apply every delta directly (start, stop, update) and drop the
  reconcile for streaming workers.

**Trade-off.** Two paths start allocations on a worker, as push and
heartbeat already did. The direct start skips an allocation the task
manager already runs, launches, stops, or holds back, and one whose task
has an unratified provisional start on the node (a payload names no
instance, so two instances of one task are not told apart and the direct
start defers to the reconcile).

**Why (performance, then simplicity).** (a) costs a heartbeat round trip
per placement before the start, which is what "within one round trip"
excludes. (b) reimplements the reconcile's stop ordering, restore
replacement, and protected-claim logic. Starting new work is the latency
that matters, and it is exactly what the push path already does.

## 29. Edge-boot provisional deltas are deferred

**Call.** Phase 2 does not move manager-side speculative dispatch and its
ratification payload onto the stream; they stay `PostWork` pushes, and an
edge start's verdict stays the `RatifyClaim` reply.

**Alternative.** The plan's provisional delta and verdict on the stream.

**Trade-off.** Speculative dispatch still needs the worker listener and a
push per claim, and the stream and the push can race for the same work
(handled by the guard in entry 28).

**Why (simplicity).** The edge-boot suites pass unmodified this way, which
is one of Phase 2's acceptance tests, and the verdict path is already one
RPC. Moving it is a protocol change to a security-relevant flow that
deserves its own change. Open in TODO.md.

## 30. A streaming heartbeat carries a position, a digest, and changed health only

**Call.** A worker on the stream sends `work_stream { epoch, seq, digest }`,
no `allocation_ids`, and only the allocation health that changed since a
reply that said `desired_via_stream`; any other reply makes the next beat
report everything again. The digest is `count:xor(splitmix64(id))`, kept
incrementally on both sides. A mismatch at the same `seq`, with nothing in
flight, queues one resync.

**Alternatives.**
- (a) Keep sending allocation ids (the plan says "only when they change").
- (b) A cryptographic digest (sha256 over the sorted ids).

**Trade-off.** The xor digest can collide by accident (two different id
sets with the same count and xor), which a cryptographic hash would make
negligible; a collision hides drift until the next real change. The
manager still makes a few per-node reads on a steady beat that do not grow
with the allocation count (checkpoint requests, content resync), so the
"0 store reads" target holds for the desired set, not for every read.

**Why (performance).** The manager ignored the reported ids already
(`build_desired_allocations` never read them), so (a) was bytes for
nothing. (b) needs the sorted set on every check; the xor updates in O(1)
per delta on both sides. Measured: 42-byte request and 7-byte reply at 300
allocations against 5,154 and 102,670 bytes for the full set.

## 31. Only job evals that place work block

**Call.** `job_submit`, `job_update`, `job_reschedule`, and `job_scale`
block. A `rollout_tick` that cannot place still fails its rollout, and the
node-scoped passes keep queuing a per-job `job_reschedule` for each job
they could not place, which is what blocks.

**Alternative.** Block every eval kind whose error carries
`PlacementBlocked`.

**Trade-off.** A rollout that runs out of room still ends `failed` and
needs an operator.

**Why (simplicity).** A rollout tick stamps its rollout `failed` before
returning, so a blocked tick would wake to a terminal rollout and do
nothing; changing rollout semantics is outside this plan. Node-scoped
evals carry no job, so they cannot be one-per-job, and their per-job retry
already lands in the right place.

## 32. The wake pass matches constraints, not capacity, and budget reports do not wake

**Call.** An event wakes a blocked eval when the node is available and the
blocked task's permit and constraints accept it (`node_could_help`), or
the key is `escaped`, or the node is the one a `node:` key names. Whether
the node has enough free room is left to the plan that runs next.
Checkpoint budget reports and `snapshot warm` completions are not events.

**Alternatives.** Check the freed resources against the task's request
before waking; add budget reports as events.

**Trade-off.** A small freed allocation on a big node of the right class
wakes a blocked eval that then blocks again: one wasted plan. An eval
blocked on checkpoint room waits for a store change on a node (an
allocation leaving it) rather than for the worker's report that its disk
freed up.

**Why (simplicity).** The plan is the only place capacity is computed
correctly (reservations, preemption, ports, disk); a second, approximate
check in the wake pass would drift from it, and a false negative would
strand work, which is worse than a wasted plan. Budget reports are worker
heartbeat data, not committed changes, so they would need a second event
source. Open in TODO.md.

## 33. Simulated workers send allocation health and stream by default

**Call.** `--simulate` nodes report every acknowledged allocation as
healthy, every beat off the stream and once on it, and take their work
over `StreamWork` unless `--sim-no-work-stream` is set.

**Alternative.** Flags defaulting off, to keep historical AWS cells
comparable.

**Trade-off.** Heartbeat-path load in scale runs from before this change
is not comparable with runs after it; `docs/worker-simulation.md` says so.

**Why (simplicity).** Real workers do both. The per-beat health write was
invisible to every scale run because the simulator never sent health (plan
§5.1), which is how it went unmeasured. A simulator that is faithful by
default is the one the next regression shows up in.

## 34. `allocation_health.updated_at` and `task_status.updated_at` mean "last changed"

**Call.** Health and status upserts write only when a value changed, so
their `updated_at` is when the value last changed, not when it was last
reported.

**Alternative.** Keep a "last reported" timestamp current on every beat.

**Trade-off.** Anyone reading `updated_at` as freshness (via `legion state
query`) now sees an old timestamp for a healthy allocation. Node liveness,
not these rows, is the freshness signal.

**Why (performance).** Writing a timestamp is what made every beat a
10,000-row changeset on a dense node; a steady beat now writes nothing.

## 35. One generic scheduler-lane group commit for instance operations

**Call.** Creates, stops, starts, destroys, resumes, suspends, forks,
ratifications, and snapshot reports all go through
`Store::scheduler_group_commit`: concurrent requests share one entry, each
inside its own savepoint, so a failing request rolls back alone
(`schedulerSettings.instance_group_commit`, default on).

**Alternative.** One coalescer per operation, as job intake and heartbeats
have.

**Trade-off.** Unrelated operations share a batch, so one slow request
delays the others in its batch.

**Why (performance, then simplicity).** The scheduler lane is one serial
pipeline whatever the op, so mixing ops is what fills batches under a real
mix (creates arriving while resumes report); per-op coalescers would split
the same traffic into smaller batches. One mechanism is one place to get
savepoint isolation right.

## 36. Op counts sum every lane's applied index, and the measured lifecycle is an instance of a running task

**Call.** `tests/raft_lifecycle_ops.rs` asserts entries per stage as the
sum of every lane's raft applied index, and prints
`legion_raft_ops_total` op names alongside. Its stages measure an instance
of a task that already runs one instance; the task's first placement (its
`task_status` flip to running) is reported as a stage of its own.

**Alternative.** Assert on the metric alone; measure a job's first
instance.

**Trade-off.** The applied index also counts entries the test did not
cause (none on an in-process store, but a background loop would show up).
The first-instance cost (3 entries) is not in the lifecycle total.

**Why (simplicity).** The metric is process-global, so parallel tests
would bleed into it; the applied index is per store and exact. Platforms
run thousands of instances of one task, so the steady-state lifecycle is
the number that scales.

## 37. The keyed-pending capacity bug was fixed in Phase 0, and the capacity pass stays as a backstop

**Call.** Phase 0's test confirmed the bug (a keyed task's `count` is
stored as 1, so only tasks with zero allocations were retried) and the fix
landed with it, in `job_configs_waiting_on_capacity`'s SQL. Phase 3 keeps
the `node_join` capacity pass, but it skips jobs with a pending or blocked
eval.

**Alternative.** Remove the capacity pass now that blocked evals exist.

**Trade-off.** A registration or uncordon still queues one cluster-wide
eval, which reads every active job; it re-plans only jobs short without an
eval, which is normally none.

**Why (simplicity).** A job can be short with no eval at all (a crash
between an instance row and its eval, a store migrated from an older
release), and nothing else would ever look at it. The pass costs one read
per join burst; dropping it trades that for a class of permanently stuck
jobs.

## 38. The raft seam test still parses only `store.rs` and `acl_store.rs`

**Call.** `tests/raft_seam.rs` was updated for the ops this work removed
(`claim_eval{,s}`, the `complete_*_and_claim_next` pair) and added
(`complete_evals`), but its file list was left alone, so the new
`unblock_evals` op in `store/blocked_evals.rs` is not classified there.

**Alternative.** Extend the test to every file under `src/storage/store/`.

**Trade-off.** Ops in the store's submodules (`fork_ops`, `keyring_ops`,
`snapshot_ops`, `suspend_ops`, and now `blocked_evals`) escape the
classification check; the capture-time ownership guard still rejects a
write on the wrong lane at runtime.

**Why (simplicity).** Extending it means classifying about a dozen
existing ops from earlier work, which is its own change. Open in TODO.md.

## 39. Session mode's user documentation is drafted in the plan, not published

**Call.** The documentation for `stateful.mode = "session"` (when to use
it, what it gives up against `keyed`, its use cases with sandboxes among
them, and the paragraph the sandbox guides gain) is written now as plan
§15, ready to move into `docs/` unchanged when Phase 6 ships. What is
public now is what exists: `docs/sandboxes-at-scale.md` and the "Choosing
a mode" section of `docs/stateful-workloads.md`, covering `count`, `slot`,
and `keyed`.

**Alternatives.**
- (a) Publish the session-mode page now, marked "planned".
- (b) Write nothing about session mode until Phase 6.

**Trade-off.** A reader of the public docs cannot learn today that a
session mode is coming or what it will cost them; the owner asked for the
trade-offs to be clear, and they are clear only in the plan until Phase 6.

**Why (simplicity).** `AGENTS.md` keeps promises of unshipped work out of
`docs/`: a page describing a mode the binary rejects would be read as a
feature that exists. (b) would leave the trade-off discussion to be
reconstructed at Phase 6 from code, after the decisions it should have
shaped. Writing the public text now, in the plan, settles the trade-offs
before the code and costs only a move when it ships.

## 40. The AWS sandbox campaign embeds its nodes on the load box and labels every projection

**Call.** `benchmarks/aws/scripts/run-sandbox-campaign.sh` runs on the rig
with no spot fleet: `legion-loadgen --mode instances` embeds its simulated
nodes (checkpoint channel faked) on the load box, against the real
3-voter control plane. It climbs a rate ladder and stops at the first rung
that does not hold, then resets the control plane and holds a density cell
on fewer, denser nodes. `analysis/sandbox_report.py` reports measured
numbers and, separately, projections (lifecycles per day, cost per million
lifecycles, resident capacity per manager), each with the measured number
it scales and its assumption.

**Alternatives.**
- (a) Run the sim nodes on the spot fleet, as the job campaigns do.
- (b) Add a KVM-capable (bare-metal) worker so sandbox boot and restore are
  real.
- (c) Report only measured numbers, no projections.

**Trade-off.** The load box runs the fleet and the load, so a very high
rung can be bound by the load box rather than the control plane (the
report's busiest-manager CPU column is how to tell). Boot and restore are
simulated, so every latency is the control plane's share. Projections can
be quoted out of context.

**Why (simplicity, then cost).** The owner asked for a baseline on a few
real servers that can run on its own. The control plane is what the
sandbox-scaling work changes, and the embedded fleet speaks the real
protocol over the real network, so four on-demand boxes (about $1.07/hour)
measure it; (a) adds a fleet and its quota for no extra signal, and (b)
costs a metal instance (about $4/hour) to measure the runtime, which is a
separate number (Phase 8 measures density per node). (c) would leave the
owner to extrapolate by hand, which is where unlabelled numbers come from;
the report does it once, with the basis printed beside every figure. Open
in TODO.md: run it on AWS.
