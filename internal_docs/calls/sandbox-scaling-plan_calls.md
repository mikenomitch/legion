# Judgement Calls: Sandbox Scaling to Very Large Session Counts

Decisions made while writing
[`plans/sandbox-scaling-plan.md`](../plans/sandbox-scaling-plan.md), before
any code exists. **Entries are ranked by project impact, highest first. The
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

## 5. Idle sessions are manifests, and archiving keyed instances ships before session mode

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

## 6. Raft places capacity envelopes, and a router fills them

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

## 7. This plan owns wake on request for idle sessions, at a gateway, not on the node

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

## 8. Unplaceable work blocks under the key that blocked it, and only matching capacity wakes it

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

## 9. Measure, then group commit, before any new semantics

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

## 10. Every phase carries p50 and p99 latency targets, measured first in Phase 0

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

## 11. Unnamed sessions converge to one, and idempotency derives the id

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

## 12. No external session registry, not even as a seam

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

## 13. Cells are deferred behind a measured trigger

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

## 14. Prefix tickets stand in for leases at the edge, and spend once

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

## 15. Per-session events stay off the replicated journal

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

## 16. Session workload identity comes from the lease, and the boundary moves to the envelope

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

## 17. The target is millions running and hundreds of millions idle per cluster, not billions running

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

## 18. Snapshot-clone density before a sub-VM tier

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

## 19. The worker's session log is a new SQLite database, not the JSON state file

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
