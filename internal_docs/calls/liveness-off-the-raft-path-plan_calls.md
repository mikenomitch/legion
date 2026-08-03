# Judgement Calls: Liveness Off the Raft Path

Decisions made while writing
[`../plans/liveness-off-the-raft-path-plan.md`](../plans/liveness-off-the-raft-path-plan.md)
(status SHIPPED). Entries marked *(Implementation)* were added while the
plan was executed; the rest are the plan's own calls. Where an implementation
entry contradicts an earlier one, it says so and gives the reason.

**Ordered most-impactful first. The entry number is its rank.**

---

## 1. Node liveness lives in the status-lane leader's memory; only transitions are replicated

**Call.** A steady-state heartbeat updates an in-memory map on the
status-lane leader and commits nothing. `node_health.last_heartbeat` and
`.last_heartbeat_unix` stop being written per beat. The `active → lost` and
`lost → active` transitions keep their existing replicated composites
(`Store::mark_nodes_as_lost`, `src/storage/store.rs:4805`;
`Store::mark_node_ready`, `:4370`), so everything a scheduler, an operator,
or a reconciler consumes stays durable.

The operator-facing model, in one sentence on the new public page:

> The manager that owns node status remembers who checked in; only changes
> are written down.

**Alternatives.** (a) **Amortize the fsync harder and stay durable** — the
option `benchmarks/aws/experiments/heartbeat-commit-probe.sh:1-31` predicted
the fix would be, and a real one: commit `f24c6e0` collected most of it for
a 26-line diff, taking gp3 from 12% to **84%** of cadence. (b) **A separate
unreplicated durable store per manager** — pay a local fsync instead of a
replicated one. (c) **Gossip/SWIM**, Consul's model.

**Trade-off.** Three things get worse, and none of them is free:

- The last-heartbeat *timestamp* becomes approximate on any manager that
  does not lead the status lane (entry 8). The *status* stays exact
  everywhere, which is the half that matters, but "approximate" is a new
  word in an operator's vocabulary that was not there before.
- Soft state is harder to reason about than replicated state. A bug in the
  liveness table is invisible to the content-hash convergence checks that
  `tests/raft_lanes.rs` uses to prove everything else correct.
- Under a standing lane split, detection now depends on a digest RPC path
  (entry 12) that does not exist today, with its own partial-failure story.

**Why (performance, then extensibility).** A heartbeat timestamp is
*derived* (the worker never sends one; the manager stamps `now`), *worthless
after a few seconds*, and — decisively — **already discarded by the code at
the one moment durability would pay off**. The startup-grace comment at
`src/manager/daemon.rs:956-964` says so outright: *"time the manager spent
down must not count as missed heartbeats: right after a manager restart
every persisted heartbeat looks stale."* Legion is paying an fsync per beat
for a value it deliberately refuses to trust. Measured, that fsync dominates:
48–86 heartbeats/sec on gp3 versus ~2000/sec on local NVMe with identical
code, where 500/sec is needed for 1000 nodes and 5,000/sec for the
campaign's top scale step.

Alternative (a) moves a constant factor — a large one, as `f24c6e0` showed —
but leaves `R` a property of the storage device, which is what commit
`a774fb0` ("Put the control-plane raft store on local NVMe") is compensating
for with hardware. Requiring ephemeral local storage for a control plane is a
bad answer to give a cloud operator, and it is the answer we give today. Even
at the post-offload 422/sec, the 5,000- and 10,000-node steps need 6× and 12×
more than gp3 delivers, so (a) buys a scale step, not the curve.

Alternative (b) is strictly dominated: an unreplicated local file does not
transfer to a new leader, so a new leader still starts empty — the same
position this call is in — while still paying the fsync and adding a second
on-disk format with its own corruption and backup story.

Alternative (c) is the right long-term answer at 10k+ nodes and is deferred,
not rejected on merit. It is deferred because it brings a membership
protocol, a key-rotation story, a new security boundary, and a network
requirement (every worker reachable from a random subset of every other
worker) that Legion does not have today — and because it does not remove the
ceiling anyway (Consul documents 5,000 client agents per datacenter). This
call makes (c) *cheaper*: gossip's output is exactly what the liveness table
holds, so gossip would replace the table's input and leave the transition,
grace, checkpoint, and read-path machinery intact.


---

## 2. `lost -> active` recovery keeps the old status-lane write; it does NOT take `mark_node_ready`

**Call.** *(Implementation; a deliberate deviation from entry 1's design,
which named `Store::mark_node_ready` as the recovery composite.)* When a
heartbeat arrives from a node the store believes is not active, it replicates
through `update_node_heartbeat_forwarding` — the path it has always taken —
rather than through `mark_node_ready`.

**Alternative.** `mark_node_ready(id, now)`, exactly as the plan specified:
the status lane, then a control-lane `availability = "available"` flip, so a
recovered node becomes schedulable in one step.

**Trade-off.** A recovered node's `node.availability` stays `unavailable`
until it re-registers, exactly as before this change. That is a real
limitation, and it is preserved rather than newly introduced — but the plan
promised to fix it here and this does not.

**Why (correctness).** `mark_node_ready_apply` writes `status = "ready"`, not
`"active"` — a pre-existing quirk of the node-join eval path. Every liveness
query in the system filters on `status = 'active'`
(`active_node_liveness_rows`, `node_status_counts`), so a node recovered
through it would silently drop out of the liveness scan **and** out of
`legion_nodes{status="active"}`. Following the plan literally would have
introduced a worse bug than the limitation it fixed. Flipping `availability`
on a heartbeat is also a larger semantic change than this plan should make:
schedulability has never been a heartbeat's to grant. The right fix —
normalizing `"ready"` to `"active"` across the node-join path — is its own
change, and is filed as an open TODO item rather than smuggled in here.

---

## 3. Phase 1 is a measurement, not a fix — and the batching knob became contingent on it

**Call.** Phase 1 ships **no behavior change**. It re-runs
`heartbeat-commit-probe.sh` post-`f24c6e0` on both storage classes and
attributes the residual gap between the fsync and the status lane's single
`pipeline_loop` thread. The `liveness.batchWindow` knob ships **only if**
`legion_store_coalesce_batch_size{path="heartbeat"}` shows batches well
below `HEARTBEAT_BATCH_MAX` under load, and it defaults to `"0ms"` if it
ships at all.

**This is a reversal recorded mid-draft.** The first draft of this plan made
phase 1 an unconditional 25 ms accumulation window on the coalescer, argued
from this arithmetic:

```
before:  commit ≈ 12ms, arrivals during a commit ≈ 48 × 0.012 ≈ 0.6
         → batch ≈ 1.6 → a stable LOW-throughput equilibrium
25ms:    commits paced at ~40/s, batch = offered/40
         → 500 offered/s is a batch of 13, inside HEARTBEAT_BATCH_MAX (128)
```

The diagnosis was right and the remedy was wrong. Commit `f24c6e0` landed
while this plan was being written and broke the same equilibrium **without a
timer**: the blocking store call was holding tokio worker threads, so
heartbeats piled up in the gRPC layer where the coalescer could not drain
them. `spawn_blocking` let the queue reach the coalescer and batches formed
on their own — **gp3 60 → 422 hb/sec, 12% → 84% of cadence**. A 25 ms window
shipped on top of that would have been a knob solving a problem that no
longer exists.

**Alternative.** Ship the window anyway. It is written, it is cheap, and
84% is not 100% — a window might close some of the remaining 16%.

**Trade-off.** Phase 1 now produces no user-visible improvement, which makes
it a hard phase to justify to anyone reading the phase list for deliverables.
It also delays phase 2 by however long the probe takes to re-run on the AWS
rig, and the rig is shared.

**Why (do not build the wrong thing twice in one week).** The offload was a
7× win that nobody predicted would be that large, arrived from a direction
the probe's own hypothesis did not name (it predicted "bigger coalescer
batches", not "the batches are starved by thread parking"), and it changed
the shape of the problem enough to invalidate a phase of this plan before
the plan was finished. That is strong evidence that the residual 16% is not
yet understood well enough to build against. The instrumentation to
understand it already exists and is already documented
(`docs/metrics.md` § "Diagnosing a manager throughput ceiling"), so the cost
of finding out is one probe run.

The architectural argument in entry 1 is **unaffected** by any of this, and
that is the point worth holding onto: `R` is still a property of the storage
device, the campaign's 5,000- and 10,000-node steps need 2,500 and 5,000
hb/sec against a measured 422, and the control plane is still pinned to local
NVMe (`a774fb0`) to buy headroom. What changed is the urgency — the fleet is
no longer 10% of margin from mass false loss — not the destination.


---

## 4. `liveness.batchWindow` ships inert, because the measurement it is contingent on has not been run

**Call.** *(Implementation.)* `liveness.batchWindow` exists in the config, is
parsed and validated, and defaults to `"0ms"` — reproducing today's
commit-while-you-can batching exactly — but **nothing consumes it**, and the
phase-1 attribution the previous entry makes it contingent on
(`heartbeat-commit-probe.sh` on gp3 and NVMe at 1000 and 4000 nodes) has not
been run.

**Alternative.** Omit the field entirely until a measurement justifies it,
which is what the plan says to do: *"shipping it blind would be adding a knob
to solve a problem that may no longer exist."*

**Trade-off.** A config field an operator can set that currently changes
nothing. Its doc comment and `configs/manager.jsonnet` both say so, and the
knob is commented out in the example config.

**Why.** The measurement needs the AWS rig, which this change cannot run.
Keeping the field means the validation rules, the config plumbing, and the
public docs are already in place for whoever runs it; leaving it out would
mean reopening `config.rs`, `manager.jsonnet`, and two doc pages to add one
`Duration`. The honest cost is one inert knob — recorded here, on the public
page, and as an open TODO item, rather than quietly shipped as though it did
something.

---

## 5. The scheduler keeps reading `node.availability`; liveness is never read across a lane boundary

**Call.** Nothing in the scheduler, the eval loop, the dispatcher, or the
rollout engine ever reads the liveness table. Placement continues to gate on
`node.availability` — a **control-lane** column — exactly as it does today
(`Store::available_node_arcs`, `src/storage/store.rs:2094`;
`available_node_ids`, `:2079`). Node loss reaches the scheduler the way it
already does: the existing `mark_nodes_as_lost` composite flips
`node.availability` on the control lane and enqueues a
`node_lost_reschedule` eval on the scheduler lane, both forwarded.

**Alternatives.** Expose the liveness table to the scheduler lane over the
manager plane, or maintain a replicated liveness read model that any lane
can query. Either would let placement react to a silent node before the
transition commits, and both are what you would design if you did not know
the current code.

**Trade-off.** Placement reacts to node loss only after the control-lane
availability flip lands, not at the instant the status leader decides. Under
a standing lane split with a slow control lane, that window is real: the
status leader knows a node is dead and the scheduler will still consider it
a candidate until the flip replicates. This is **not a new** window — it is
exactly today's behavior, unchanged — but this call is where it is
deliberately preserved rather than fixed, so it belongs on the record.

**Why (simplicity).** The multi-raft split already put schedulability
(`node.availability`, control lane) and liveness (`node_health`, status
lane) in different places, and `set_node_availability`'s own doc comment
names availability as *"the scheduler's single placement chokepoint"*
(`src/storage/store.rs:4570-4573`). The lanes complication that looked like
the hardest part of this plan turns out to be already solved by a decision
someone else made for other reasons. Adding a cross-lane liveness read would
create the first place in Legion where a scheduling decision depends on
another lane's leader being reachable — a new failure mode, bought to close
a window that has never been reported as a problem.


---

## 6. On election, the new leader seeds every active node at `now` — never at its persisted heartbeat

**Call.** When a manager acquires status-lane leadership, it reads the set of
registered active nodes from replicated state and stamps every one of them
`now` in the liveness table, then marks nothing lost for `failoverGrace`.
The persisted `last_heartbeat_unix` values, and the checkpoint written by
the previous leader, are **not** consulted.

**Alternative.** Seed from the checkpoint. It is right there, it is at most
`checkpointInterval` stale, and it carries real information — a node that
genuinely died four minutes before the election would be caught immediately
instead of `failoverGrace` later. This is the option a reviewer will
propose, because throwing away known-good data feels wasteful.

**Trade-off.** Real information is discarded on every election. A node that
died well before the handover gets an extra `failoverGrace` (60s) of life,
and its allocations are rescheduled that much later. That is the price, and
it is paid on every election whether or not any node actually died.

**Why (correctness).** The checkpoint interval is 30s and the default lost
window is 10s, so a checkpoint is stale by *three windows* by construction.
Seeding from it would mark most of the fleet lost on the new leader's first
tick — precisely the mass false loss this plan exists to prevent, arriving
through the back door. Nomad reaches the same conclusion from the same
constraint: `initializeHeartbeatTimers()` arms every node at
`failover_heartbeat_ttl` rather than at any remembered value. Discarding the
data is what makes the checkpoint safe to write at all (entry 10).


---

## 7. `failoverGrace` defaults to 60 seconds, not Nomad's 5 minutes

**Call.** `liveness.failoverGrace: "60s"`.

**Alternatives.** (a) Nomad's `failover_heartbeat_ttl`, **5 minutes**. (b)
One lost window (10s), which is theoretically sufficient — every live worker
checks in within one interval.

**Trade-off.** An election that takes unusually long, or one that leaves a
meaningful fraction of workers deep in their retry backoff, could still
produce loss at 60s where 5 minutes would not. If that happens in a real
incident it will look like a tuning failure, and the metric that proves it
(`legion_liveness_loss_suppressed_total{reason="failover_grace"}` firing for
real losses rather than phantom ones) has to be read to tell the difference.

**Why (simplicity, balanced against detection).** Nomad's 5 minutes is
calibrated to Nomad's defaults — a 10–20s effective TTL plus a 10s grace, so
5 minutes is roughly 15× its own window. Legion's window is 10s, so copying
the number would mean **30×**, a detection regression an operator would
notice and would not have asked for. 60s is a sixth of Nomad's value and
still comfortably covers an election plus `HEARTBEAT_RETRY_MAX` (10s) plus a
slow reconnect. Option (b) is too tight for the same reason: it leaves no
room for the worker-side backoff that an election itself causes.

Validation rejects `failoverGrace` below the lost window at config load,
because a grace shorter than the window is not a grace.


---

## 8. A manager that does not own liveness shows an approximate heartbeat time and says so, rather than forwarding the read

**Call.** The status-lane leader replicates a batched liveness checkpoint
every `checkpointInterval` (default 30s) recording each node's **observed**
last-beat time. Every other manager serves that value locally and labels it.

```console
$ legion node list
ID          POOL     STATUS   AVAILABILITY  LAST HEARTBEAT
node-0007   default  active   available     3s ago
node-0008   default  active   available     ~24s ago (checkpoint)
```

`NodeMsg.heartbeat_source` (`"live"` | `"checkpoint"` | `""`) carries it to
`--json`, the UI, and MCP consumers.

**Alternatives.** (a) Forward the read to the status-lane leader, so every
manager returns the exact value. (b) Leave the column blank on non-leaders
and let the status column carry the whole answer. (c) Gossip the timestamps.

**Trade-off.** Two managers can print different values for the same node.
Someone will file that as a bug, and the answer will be "working as
intended", which is never a satisfying reply. Sorting by `last_heartbeat`
(`src/manager/server.rs:5398`) also now sorts on whatever value *that*
manager holds, which is a subtly different ordering per manager.

**Why (performance on a hot read path, and honesty).** `get_nodes` is a pure
local read today, with ACL filtering, query matching, sorting, and pagination
applied *after* the fetch (`src/manager/server.rs:5372-5395`). Option (a)
would forward the entire pre-filter node list on every `legion node list`
and every UI poll — turning a display field into a hard dependency on the
status leader being reachable, on a page an on-call responder opens
precisely when things are broken. Option (b) is defensible and is offered as
`checkpointInterval: "0s"`, but a blank column reads as a bug to a user who
does not know why it is blank, whereas `(checkpoint)` reads as an
explanation. The checkpoint costs 2 commits per minute against today's ~500
per second — a ~15,000× reduction — so the accuracy is bought for
effectively nothing.

The half that does **not** regress is worth stating because it is what
protects the on-call responder: `node_health.status` stays replicated, so
`legion node list`, `legion_nodes{status}`, and the UI status pill are
exact on every manager. Only the *time* is approximate.


---

## 9. `heartbeat_source` is its own `NodeMsg` field, not a suffix baked into `last_heartbeat`

**Call.** *(Implementation of the previous entry's surface.)* A new
`string heartbeat_source` (`"live"` | `"checkpoint"` | `""`) on `NodeMsg`. The
CLI table composes the two at print time; `--json` emits both separately; the
UI renders it as a tooltip on the relative time rather than as a column.

**Alternative.** Append `(checkpoint)` to `last_heartbeat` on the wire. No new
field, and every existing client — including ones built before this change —
would show the caveat for free.

**Trade-off.** A client built before this field ignores it and shows an
approximate timestamp as though it were exact, for up to
`checkpointInterval`. Accepted: the node's *status* is exact on every client
either way, and status is what anything automated should branch on. The
caveat is stated in the proto comment, in `docs/agent-readiness.md`, and on
the public page.

**Why (extensibility).** `last_heartbeat` is a timestamp that agents, the UI,
and `legion node list --sort-by` all parse. Making it sometimes-not-a-timestamp
breaks every consumer to serve a display concern, and it cannot be undone
without another breaking change. This also settles the plan's open question 5
(UI or `--json`?) as *both, quietly*.

---

## 10. A checkpoint is written for display and is never read back as liveness

**Call.** The liveness checkpoint is write-only from the system's point of
view. Nothing in the lost-node decision, the election seed, or any
reconciler ever reads it. The rule is stated in the code comment at the
write site, in the proto comment, and on the public page.

**Alternative.** Read it on election (entry 6's rejected alternative), or as
a warm-start for a restarting manager. It is the obvious use for a value the
system went to the trouble of writing.

**Trade-off.** Deliberately persisting state that the system refuses to
trust is a smell, and it is the kind of invariant that erodes: the next
person to touch this code will see a timestamp column and a leader with no
liveness state and will connect them. Mitigated the only way it can be —
by naming the rule at every site that could break it, and by a unit test
(`checkpoint_is_never_read_as_liveness`) that seeds a stale checkpoint,
forces an election, and asserts zero losses.

**Why (correctness beats tidiness).** A 30s-stale checkpoint under a 10s
window is not a warm start, it is a fleet-wide kill signal. The value of the
separation is that it lets the checkpoint interval be chosen for *display*
quality alone, with no correctness consequence — which is why 30s is an
acceptable default rather than something that would have to be tuned against
the lost window.


---

## 11. The checkpoint is an upsert guarded on `status = 'active'`, not a bare UPDATE

**Call.** *(Implementation.)* `checkpoint_node_heartbeats_apply` inserts the
health row when a registered node has none, and refreshes the timestamp only
when the existing row already says `active`.

**Alternative.** A plain `UPDATE ... WHERE node_id = ? AND status = 'active'`,
which is the minimal expression of "display state only" and cannot create
anything.

**Trade-off.** The checkpoint can now create a row, which is one more thing it
can do than its name suggests. Bounded by its `SELECT ... FROM node` guard: it
can only ever create a row for a registered node, carrying exactly the values
hydration already assumes for a missing row.

**Why (correctness).** Before this work, a `node_health` row lost to a crash
between registration's two halves was recreated by that node's next heartbeat.
With heartbeats in memory, a bare UPDATE would leave the row missing forever —
and with it that node's `lost_after_seconds`, silently reverting it to the
default window. The upsert restores the old self-healing property at
checkpoint cadence instead of at beat cadence, which is the least surprising
place for it to live.

---

## 12. Managers push liveness digests to the status leader; the leader never pulls

**Call.** Each manager that does not lead the status lane batches the node
ids it personally received heartbeats from and sends **one**
`ReportNodeLiveness` RPC per `digestInterval` (default 1s). The leader merges
them into its table. Cost is `managers × 1 RPC/sec`, independent of fleet
size.

Paired with the rule that makes it safe:

> When a peer manager's last digest is older than `peerDigestGrace`
> (default 30s), the node ids that peer last claimed are **not eligible to
> be marked lost**. The leader logs once per peer per gap and raises
> `legion_liveness_peer_stale{peer}`.

**Alternative.** Every manager tracks its own beats and the leader unions
them at tick time — a pull. It needs no new periodic traffic, it is
naturally consistent with the tick, and it is what you would write first.

**Trade-off.** The leader acts on data up to `digestInterval` old, and the
design now carries a suppression rule that can, in the worst case, keep a
genuinely dead node alive for `peerDigestGrace` after its manager also goes
quiet. There is also standing traffic in steady state where the pull has
none.

**Why (fail-safe over fail-dangerous).** A pull makes the **lost decision
itself** depend on an RPC completing. A manager that is slow, restarting, or
partitioned at tick time contributes nothing, and its share of the fleet
looks dead — turning a manager-to-manager problem into fleet-wide node loss,
which is the exact failure class this plan exists to eliminate. A push
inverts it: the leader holds the last digest, knows how old it is, and can
decline to decide. That is the same reasoning as Kubernetes'
`stateFullDisruption` and as the cadence plan's mass-disruption circuit
breaker, applied one level down — and it composes with that breaker rather
than duplicating it, since peer-gap suppression catches the partial case and
the breaker catches the total one.


---

## 13. Peer-gap suppression is bounded on BOTH sides

**Call.** *(Implementation; resolves an ambiguity in the previous entry.)* A
peer manager's claimed nodes are suppressed from the lost decision while that
peer's last digest is between one `digestInterval` and one `peerDigestGrace`
old. **Past the grace, its nodes become eligible again.**

**Alternative.** Suppress indefinitely while the peer is silent — the reading
the previous entry's prose most naturally supports ("not eligible to be marked
lost until it reports again").

**Trade-off.** A manager that is genuinely gone takes its share of the fleet
with it after 30 seconds, so a dead manager *does* eventually produce node
losses.

**Why (correctness).** Unbounded suppression turns a manager failure into a
permanent hole in the fleet's failure detection: a dead manager's nodes could
never be marked lost, however long they had been gone. And the workers dialing
that manager genuinely are unreachable — their allocations should move. The
plan's own testing section resolves the ambiguity the same way (*"at
`t + peerDigestGrace + 1s` C's become eligible"*), so this follows the tests
rather than the prose.

---

## 14. `ReportNodeLiveness` goes on `RaftTransport`, not on the `Manager` service

**Call.** The digest RPC is added to the existing authenticated manager-plane
`RaftTransport` service (`src/storage/raft/server.rs:70`), alongside
`ForwardLaneWrite`. It takes no ACL resource/action/scope and no row in
`tests/acl_endpoints.rs`.

**Alternative.** Put it on the `Manager` service with an ACL row
(`AclResource::Node` / `AclAction::Write` / worker-plane-style enforcement),
which is what `AGENTS.md`'s "every Manager gRPC endpoint must enforce ACLs"
rule would produce if the message lived there.

**Trade-off.** `tests/acl_endpoints.rs::every_manager_rpc_is_classified`
parses `proto/legion.proto` for `Manager` RPCs and fails the build on an
unclassified one. Putting the digest outside that service puts it outside
that safety net, so a future mistake here — a second manager-plane RPC that
should have been authenticated and is not — will not break the build. The
mitigation is a proto comment stating the exemption and its reason
explicitly, so the next reader does not have to infer it from absence.

**Why (the message is not a user surface).** A liveness digest is manager
internals: it carries no user intent, no tenant scope, and nothing an ACL
vocabulary can meaningfully express — `node`/`write` scoped to what, exactly,
when the payload is "here is who I heard from"? Putting it on `Manager` would
mint a user-visible RPC for an internal message and force an ACL
classification that is a lie. `RaftTransport` already has the right trust
model (shared-secret peer auth, `PROTOCOL_VERSION`-versioned payloads) and
the right degradation story against an older peer, which entry 15 depends on.


---

## 15. Version skew is absorbed entirely on the manager plane; the worker protocol does not change

**Call.** No `HeartbeatRequest`/`HeartbeatReply` field, no
`protocol_version` bump, no worker behavior change. A manager that gets
`unimplemented` from `ReportNodeLiveness` on a peer falls back to today's
per-heartbeat `ForwardLaneWrite::NodeHeartbeat`
(`src/storage/raft/forward.rs:54`), counting
`legion_liveness_digest_rpcs_total{outcome="unimplemented"}`.

**Alternative.** Have workers help — for example, send a monotonic beat
counter or a node epoch so the leader can rebuild state faster after an
election, or address heartbeats to the status leader directly via a leader
hint on the reply.

**Trade-off.** The leader rebuilds liveness purely from arrival times, which
means it cannot distinguish "this worker has been beating steadily for an
hour" from "this worker just came back" without consulting replicated
status. That is enough for every decision in the plan, but it forecloses
some future refinements (per-node beat-loss statistics, say) without a later
protocol change.

**Why (upgrade blast radius).** `docs/upgrades.md:33-37` bumps
`protocol_version` only for breaking worker-plane changes, and `:54-56` keeps
heartbeats out of the handshake entirely *"to keep that hot path lean"*.
Workers are the numerous, slow-moving, operator-scheduled half of a Legion
fleet — 1000 of them, upgraded "at whatever pace suits the workloads". A
change that requires none of them to move is worth a real amount of design
constraint. It also means the whole plan can be rolled back by downgrading
managers alone, which is what makes entry 17's no-migration property
actually usable.


---

## 16. The failover-grace fix moves from the cadence plan into this one

**Call.** The status-lane leadership grace
(`internal_docs/TODO.md:369-378`, currently assigned to phase 3 of
`heartbeat-cadence-at-scale-plan.md`) becomes **phase 2 of this plan**. The
mass-disruption circuit breaker stays with the cadence plan, and this plan
consumes it rather than reimplementing it. The cadence plan is not edited —
it is uncommitted work owned by another session — so the cross-reference is
one-way, from here.

**Alternative.** Leave it where it is and take a dependency on it. The
cadence plan is written, the item is scoped there, and moving work between
plans creates exactly the drift the repo's plan rules exist to prevent.

**Trade-off.** Two plans now describe the same hazard, and the cadence plan's
phase 3 will read as if it still owns something it does not. A reader who
opens only that plan gets a stale picture. Mitigated by §0 of this plan,
which states the ownership split as a table and names this item as the one
that moves — but a one-way cross-reference is strictly worse than a
reciprocal one, and that is accepted rather than solved.

**Why (the fix is load-bearing here, incidental there).** In the cadence
plan the grace is a safety valve: a nice-to-have that makes adaptive
back-off less sharp. Here it is the **rebuild mechanism** — once liveness is
leader memory, a new leader inherits nothing, so without the grace phase 3
marks the entire fleet lost on the first election after it ships. It cannot
be a dependency on another plan's phase 3 when it is the precondition for
this plan's phase 3 being safe at all. Shipping it as phase 2 also means it
lands as a standalone fix for a hazard that is live *today*, which is worth
doing whether or not the rest of this plan ever ships.


---

## 17. No schema migration

**Call.** `node_health.last_heartbeat` and `.last_heartbeat_unix` keep their
columns, their types, and their names. The only change is who writes them:
the checkpoint timer instead of every heartbeat.

**Alternative.** Drop `last_heartbeat_unix` (nothing reads it for decisions
any more), or add a `liveness_source` column so the freshness marker is
persisted rather than computed per-manager, or move the display timestamp to
a separate table that is obviously not the liveness record.

**Trade-off.** Two columns keep names that now mean "as of the last
checkpoint" rather than "the last heartbeat", which is mildly misleading to
anyone reading the raw database or a `legion state` dump
(`src/cmd/state.rs:34`). A future reader could reasonably assume the column
is authoritative. The column comment and the public page have to carry that
weight, which is weaker than a name doing it.

**Why (compatibility).** A plan that changes no on-disk format has no
downgrade hazard beyond staleness, and staleness resolves itself within one
lost window once heartbeats resume writing through raft. That property is
what makes the rollback story in §10 a single paragraph instead of a
procedure, and it is the largest compatibility asset in the plan — worth
more than a tidier column name. It also means phases 1 through 4 can each be
reverted independently without a database step, which is what allows the
phasing in the first place.


---

## 18. The liveness table lives on `Store`, not on the manager service

**Call.** *(Implementation.)* `LivenessTable` (`src/storage/liveness.rs`) is
an `Arc` field on `Store`, and `Store::update_node_heartbeat` remains the one
entry point a heartbeat takes.

**Alternatives.** (a) Put it on `ManagerDaemonService` — it is arguably
manager state, not storage state, and `src/storage/` is where *persistence*
lives. (b) A standalone service owned by `run_manager` and threaded into both
the gRPC handler and the checker loop.

**Trade-off.** `Store` gains a field that is explicitly not persisted, in a
module named `storage`. Accepted, and stated in the module's first paragraph.

**Why (simplicity).** Three call sites need the table and all three already
hold a `Store` and nothing else: the heartbeat handler, the status-checker
tick, and `RaftTransport::ReportNodeLiveness` (which only has a `Store`
because `forward_store` was already there for `ForwardLaneWrite`). `Store`
also already carries process-local soft state that is neither persisted nor
replicated — the two coalescers, `node_health_orphan_candidates` — so this is
the established shape rather than a new one. Options (a) and (b) both mean
threading a second handle through code that has no manager service at all,
including `tests/raft_lanes.rs` and every embedded use of `Store`.

---

## 19. The registry gained a float gauge rather than scaling the ratio into an integer

**Call.** *(Implementation.)* `src/manager/telemetry.rs` gained a `FloatGauge`
family type and a `SampleValue::GaugeF64` variant, so
`legion_heartbeat_freshness_ratio` reads `0.22`.

**Alternative.** Export `legion_heartbeat_freshness_permille` as an integer
(`220`) — no new metric type, no new arm in the OTLP export.

**Trade-off.** One more type in the registry and one more branch in
`src/otel.rs`.

**Why (simplicity at 3am).** The whole point of the ratio is that it is in the
same units as its danger threshold: false loss begins at
`served_interval / lost_window`, which is `0.20` at the defaults. An operator
comparing `220` against `0.20` has to do a conversion mid-incident, and every
doc, alert rule, and log line would have to carry the factor. Prometheus
gauges are floats; the registry was the thing that was wrong.
