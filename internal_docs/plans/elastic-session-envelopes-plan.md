# Plan: Elastic Session Envelopes

> **Judgement calls:** [`calls/elastic-session-envelopes-plan_calls.md`](../calls/elastic-session-envelopes-plan_calls.md)

**Status: IN EXECUTION.** Phase 1 (node-driven grow and shrink between a
floor and a ceiling, one scheduler-lane write per resize) is built and
tested. Phase 2 (the scheduler reclaims the elastic part of an envelope
for higher-priority work) and Phase 3 (a measured, fleet-level resize
budget) are planned. Written 2026-10-13. Builds on session mode
([sandbox-scaling-plan.md](sandbox-scaling-plan.md) §6.2, Phase 6) and on
[session-durability-and-routing-plan.md](session-durability-and-routing-plan.md).
Deliberately deferred and named: preemptible elastic room (Phase 2), a
tunable headroom (calls entry 4), and a resize that preempts to grow
(non-goal).

---

## 1. Problem statement

A session task reserves capacity per node as an **envelope**: a generated
`<task>-envelope` allocation on every eligible node, sized by
`stateful.envelope.resources` (`src/structs/session.rs:270`). Sessions are
admitted against the envelope's room, never against the node's free
capacity (`src/worker/task_manager.rs:3531`). The scheduler counts the
envelope like any allocation, so capacity accounting stays honest.

The envelope holds its full size whether sessions fill it or not, and an
operator has to pick one number that is wrong at both ends of the day:

- **Sized for the peak, it strands capacity off-peak.** A platform whose
  agent sandboxes peak at 54 concurrent sessions per node at 2pm and drop
  to 14 at 4am holds room for 60 all night. Keyed sandboxes, services, and
  batch work on the same nodes cannot use the 46 idle slots. In the
  simulation in §9, a static envelope at the peak strands 26 of its 60
  slots on average over the day (44%).
- **Sized for the average, it refuses the peak.** A smaller envelope
  answers `RESOURCE_EXHAUSTED "every envelope ... is full"` once sessions
  fill it, even when the node has free room nobody is using
  (`src/manager/sessions.rs:1176`). `docs/session-mode.md` tells the
  operator to "size envelopes for the peak you want to absorb", which is
  the first bullet.

What an operator does today instead: pick the peak and eat the waste, or
run a cron job that edits `envelope.resources` and resubmits the job. A
resubmit replaces every envelope (a rollout), which is a full scheduler
pass and a raft entry per node, and the sessions in the old envelopes get
30 seconds before they are ended (`docs/session-mode.md`, "Ending
sessions").

## 2. Prior art

- **Kubernetes in-place pod resize** (`InPlacePodVerticalScaling`, beta in
  1.33, [KEP-1287](https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/1287-in-place-update-pod-resources)).
  A pod's requests change without a restart. The kubelet admits the resize
  against the node's allocatable capacity and reports `Deferred` or
  `Infeasible` when it does not fit. We take the shape: the node is the
  authority on whether the new size fits, a refused grow is a status, not
  an error, and the reservation the scheduler sees moves with the resize.
- **Kubernetes Vertical Pod Autoscaler**
  ([docs](https://kubernetes.io/docs/concepts/workloads/autoscaling/vertical-pod-autoscale/)).
  Recommends requests from observed usage, bounded by `minAllowed` and
  `maxAllowed`. We take the floor and ceiling. We reject its
  recommender-as-a-separate-controller model: VPA reacts in minutes, and a
  session envelope has to grow within seconds of filling.
- **Borg Autopilot**
  ([EuroSys 2020](https://dl.acm.org/doi/10.1145/3342195.3387524)).
  Limits follow usage with a margin, and lowering a limit is slower and
  more cautious than raising it. We take the asymmetry: grow at once,
  shrink only after `shrinkAfter` of sustained low use.
- **Nomad** has no in-place resource change. Any change to `resources`
  creates new allocations. Envelopes inherit that today.

## 3. Jobs to be done, by persona

**Cluster operator**
- When my agent sandboxes are quiet at night, I want the room they are not
  using to go back to the cluster, so my batch jobs and other sandboxes can
  run on the same machines.
- When a burst of sessions arrives, I want the envelope to take free room
  on the node rather than refuse, up to a ceiling I set, so one tenant
  cannot eat a whole node.
- When I look at `legion node usage`, I want it to show what the envelope
  holds right now, so capacity numbers stay true.

**Job author / platform builder**
- When I set up a session task, I want to say "always keep this much, never
  take more than that", so I do not have to guess one number for the whole
  day.
- When I need guaranteed burst room, I want to keep the old fixed envelope,
  so nothing about my capacity changes behind my back.

**On-call responder**
- When sessions are refused, I want to see whether the envelope is at its
  ceiling, still growing, or blocked by a full node, so I know whether to
  wait, raise the ceiling, or add nodes.

*Not in scope:* the autonomous agent persona gets nothing new to call.
Resizes happen without an API call, and the existing
`legion session nodes` and `node usage` reads show the current size.

### User scenarios

1. A platform sets `resources = { cpu = 6000, memory = 6144 }` and
   `max = { cpu = 30000, memory = 30720 }`. At 4am each node holds about
   7,000 MHz for its sessions. By 2pm the envelopes have grown in steps to
   30,000 MHz. By 6pm they start handing room back, ten minutes after use
   drops.
2. A node is nearly full of keyed sandboxes when sessions on it pick up.
   Its envelope grows to what fits and stops there. The router sends the
   rest of the starts to nodes with room, and `legion_envelope_resizes_total{outcome="refused"}`
   counts the node that could not grow.
3. A platform that needs 30 sessions of instant headroom per node at any
   hour sets the floor to cover it, or omits `max` and keeps a fixed
   envelope.

## 4. Goals and non-goals

**Goals**
- An envelope sizes itself between a floor and a ceiling, growing when its
  sessions approach its size and shrinking after they drain.
- Every resize goes through the replicated store, so the scheduler, plan
  apply, preemption, `node usage`, and blocked-eval wakes see the size the
  node admits against.
- A shrink can never leave running sessions over their envelope, and a
  grow never commits room the node does not have.
- A fixed envelope stays the default (omit `max`), and `max =
  resources` is the same thing.
- Bounded control-plane cost: no entry when nothing changes, one
  scheduler-lane entry per resize, and a refused grow writes nothing.

**Non-goals**
- Growing by preempting other work. A grow takes free room only.
- Sizing from anything but active sessions. Suspended sessions hold no
  room, and node-local warm members (if a warm pool lands) are reclaimable,
  so they must not inflate the envelope.
- Per-session vertical resize. Sessions keep their declared size.
- Changing how the router scores nodes.

## 5. Background: what exists today

- **The envelope task** is generated at parse time with `resources =
  envelope.resources` and `count = { num = 1, per = "id" }`
  (`src/structs/session.rs:270`). The scheduler places it like any
  allocation and stores its size on the allocation row
  (`allocation.resources`, `src/structs/ledger.rs:13`).
- **The router reads the row**, not the task config, for each envelope's
  room (`src/manager/sessions.rs:880`), and so does `legion session
  nodes`. Both pick up a resized row with no change.
- **The node admits against the task config's `resources`**
  (`src/worker/task_manager.rs:3546`), which the payload carries as
  `TaskAllocation.task_resources` (`src/manager/dispatcher.rs:592`). It
  never sees the row.
- **No in-place allocation resize exists.** Sandbox-scaling calls entry 11
  avoided one for promotion. But an in-place `UPDATE` of an allocation row
  already flows through every derived structure: the changeset projection
  treats it as an upsert (`src/storage/store.rs:11444`), the read model
  splices it and adjusts the node's usage memo (`cache_apply_allocation_delta`),
  the work log marks it dirty so a streaming worker gets the new payload
  (`src/storage/work_log.rs:217`), and the persisted `node_alloc_usage`
  cache is invalidated by a node epoch bump (`node_alloc_fingerprint_tx`).
- **Worker reports ride the heartbeat**, and scheduler-lane writes from a
  heartbeat on a follower are forwarded to the lane leader
  (`src/storage/raft/forward.rs`), the same way snapshot reports are.
- **Blocked evals wake on capacity events**, which the projection derives
  from node changes and allocation deletes only
  (`src/storage/store.rs:11717`). An in-place shrink frees room without a
  delete, so it would not wake anything.
- **Heartbeats run every 2 s** on fleets up to 2,500 nodes
  (`src/constants.rs:141`), which bounds how fast a node can ask to grow.

## 6. Proposed design

### 6.1 The surface

```nickel
stateful = {
  mode = "session",
  envelope = {
    resources = { cpu = 6000, memory = 6144 },     # the floor: always held
    max = { cpu = 30000, memory = 30720 },          # the ceiling: grows up to this
    shrinkAfter = "10m",                            # default
  },
},
resources = { cpu = 500, memory = 512 },            # one session
```

- `max` must size exactly the keys `resources` sizes, each at or above the
  floor. Without it, or equal to the floor, the envelope is fixed.
- `shrinkAfter` (default `10m`, at least `1m`) is how long an envelope's
  sessions must use less than half of it before it gives room back. Only
  with `max`.

`job validate` refuses, each with a hint:

```text
Error: task workspace: stateful.envelope.max must size exactly the keys envelope.resources sizes (cpu, memory), and it does not size memory
Hint: set max.memory, at or above resources.memory
Error: task workspace: stateful.envelope.max.cpu = 4000 is below the floor envelope.resources.cpu = 6000
Error: task workspace sets stateful.envelope.shrinkAfter without envelope.max. A fixed envelope never shrinks
Error: task workspace: stateful.envelope.shrinkAfter "30s" is shorter than 1m
```

### 6.2 Who decides: the node

The node decides when to resize, because it is the only party that knows,
at every instant, what its sessions hold. That matters most for a shrink:
a manager reading the session index sees usage as of the last heartbeat,
and sessions started at the edge between that report and the shrink would
end up over a smaller envelope, on room the scheduler has already handed
to someone else.

Per envelope, at each heartbeat, the node's controller
(`src/worker/envelope_elastic.rs`) reads the active sessions' usage `u`,
the committed size `s`, the floor, the ceiling, and one session's shape.
Fill is `u / s`, the highest over the envelope's keys.

- **Grow** when fill exceeds **0.9**, or one more session would not fit,
  and the envelope is below its ceiling on some key. The target is the size
  at which the sessions fill **75%** of it, at least one session more than
  they hold now, clamped to the floor and the ceiling. A grow never lowers
  any key.
- **Shrink** when fill stays below **0.5** on every key for `shrinkAfter`,
  to the same 75% target. A shrink never raises any key.
- **One request in flight per envelope**, re-sent on every heartbeat until
  the manager answers it.

The thresholds are constants in Phase 1 (calls entry 4). The band between
0.5 and 0.9 is the hysteresis that keeps an envelope from flapping: a grow
lands at 0.75, and it takes a 33% drop in use, held for `shrinkAfter`, to
shrink again.

### 6.3 How a resize is written

The node puts `EnvelopeResize { allocation_id, from, to }` on its
heartbeat. The manager that receives it runs one scheduler-lane op,
forwarded to the lane leader when it does not lead
(`ForwardableLaneWrite::ResizeEnvelope`):

1. Load the allocation. It must be live, on this node, and an envelope.
   Otherwise the answer is `gone`.
2. Check `to` against the session task's floor and ceiling in the
   envelope's own job config. A node cannot resize outside them
   (`invalid`).
3. Compare-and-set on `from`. If the row already holds `to`, the answer is
   `applied` (a retried request). If it holds anything else, the answer is
   `stale` with the committed size.
4. **Grow:** the node must be `available`, and the new size must fit the
   node's free capacity with the envelope's current size given back,
   recomputed from committed rows inside the transaction. When the whole
   grow does not fit, it takes what does on each key (`partial`). When no
   key can grow, the answer is `refused` and nothing is written.
5. `UPDATE allocation SET resources`, and bump the node and job-config
   allocation epochs. The epoch bump is what makes a plan that sampled the
   node before the grow fail its compare-and-set and replan, and what
   invalidates the persisted `node_alloc_usage` cache.

The answer rides back on the same heartbeat reply, with the committed
size. The new size also reaches the node as a changed payload
(`TaskAllocation.envelope_resources`), through the work stream or the next
desired set.

### 6.4 A shrink never strands a running session

Before the node asks to shrink, it caps its own admission at the target:
starts and resumes see `min(size, target)` as the room. It keeps the cap
until the manager answers. Because the target is computed from usage under
the same lock that admits, and admission cannot pass the cap, usage is at
or below the target when the shrink commits. A heartbeat that fails keeps
the cap and re-sends the request. `stale`, `refused`, `gone`, and
`invalid` lift the cap.

One more race: a follower can answer a heartbeat from a replica that has
not applied the resize yet, so the desired set in that reply can carry the
old, larger size. For 60 seconds after any resize answer, the node trusts
its own record of the size over the payload's. Past that, the payload
wins (it is also how a restarted worker learns the size).

### 6.5 Freed room wakes blocked work

The changeset projection emits a `freed` capacity event when an
allocation `UPDATE` lowers any amount in its `resources`. That covers a
shrink, and also the existing suspend path that rewrites a suspended
instance's resources to its reserve.

### 6.6 What the router says

When a start of an elastic task is refused for room, the router's refusal
says the envelopes are growing and to retry in a few seconds, instead of
"raise stateful.envelope.resources". The router reserves room at the pick
and asks the least full node even when its view says every envelope is full
(PR #43), so the refusal this changes is usually "no node accepted the
session" after the nodes themselves said no:

```text
every envelope for agents/workspace is full (12 nodes); envelopes below
stateful.envelope.max grow within a heartbeat as nodes report their use,
so retry in a few seconds, or raise stateful.envelope.max
```

## 7. Proto, struct, config, and persistence changes

- `TaskAllocation.envelope_resources` (string, JSON): an envelope's
  committed size, from its allocation row. Empty on every other payload.
- `HeartbeatRequest.envelope_resizes` (repeated `EnvelopeResize`) and
  `HeartbeatReply.envelope_resizes` (repeated `EnvelopeResizeResult`).
- `SessionEnvelopeConfig.max` and `.shrink_after`
  (`src/structs/session.rs`), in `configs/jobs/maximal.ncl`.
- `ForwardableLaneWrite::ResizeEnvelope` on the manager plane (serde, same
  `PROTOCOL_VERSION` rules as the other variants).
- Metric `legion_envelope_resizes_total{direction, outcome}`.
- No migration: the size lives on the existing `allocation.resources`
  column.

## 8. Implementation phases

**Phase 1: node-driven elastic envelopes (this change).** Everything in §6.
Ships alone: a job that sets `max` gets elastic envelopes, every other job
is unchanged.

**Phase 2: preemptible elastic room.** A higher-priority placement that
does not fit a node could shrink an envelope's elastic part (the room
above its floor and above its sessions' use) instead of evicting whole
allocations. Needs the preemption planner to model a partial victim and a
two-step handshake with the node (cap, then shrink), since the node is the
only one who knows usage. Trigger: operators reporting keyed placements
blocked behind grown envelopes for longer than `shrinkAfter`.

**Phase 3: a fleet resize budget.** If a fleet-wide burst makes every node
grow in the same heartbeat, the scheduler lane takes one entry per node
within 2 seconds (1,000 nodes, 500 entries a second for one beat). Group
commit absorbs some of it. If that is ever measured to delay plans, add a
per-manager coalescer that folds every queued resize into one entry.

## 9. Testing plan

- **Unit, controller** (`src/worker/envelope_elastic.rs`): grow at 0.9,
  grow when one session does not fit, target at 0.75 clamped to floor and
  ceiling, shrink only after `shrinkAfter` of sustained low use, never past
  usage, one request in flight, backoff after `refused`, cap lifted on
  every terminal answer.
- **Unit, worker** (`src/worker/task_manager.rs`): admission uses the
  committed size and the shrink cap, a grow answer raises the room, a
  stale payload inside the trust window is ignored.
- **Unit, spec** (`src/structs/session.rs`): every refusal in §6.1.
- **Integration** (`tests/integration/session_integration.rs`, fake nodes, single-voter
  raft): a heartbeat grow writes one entry and the router sees the new
  room, a grow on a node full of other work is `refused` with no entry, a
  partial grow takes what fits, a retried request is `applied` with no
  second entry, a shrink wakes a blocked eval, a resize outside the bounds
  is `invalid`.
- **Simulation** (`elastic_envelope_diurnal_measure`, ignored by default):
  the shipped controller over two simulated days of a 4:1 diurnal load, a
  static envelope at the peak against an elastic one, reporting stranded
  capacity, refusals, and resizes per day.

## 10. Rollout and backward compatibility

A job without `max` behaves exactly as before. Legion has no installed
base, so there is no version gate: a worker without the controller never
asks to resize, and a manager without the handler ignores the field. Mixed
versions inside one cluster fall back to fixed envelopes.

## 11. Open questions

- **Should headroom be a knob?** Lean no until a platform asks: the floor
  already buys guaranteed burst room (calls entry 4).
- **Should suspended `local` sessions count toward the shrink decision?**
  Lean no: they hold no room, and counting them would keep an envelope
  large for sessions that are not running. A resume into a shrunk envelope
  is a retryable refusal that triggers a grow.

## 12. Acceptance criteria

- A job with `max` grows its envelopes when sessions fill them and shrinks
  them after `shrinkAfter`, and `legion node usage` shows each size.
- A grow never takes room the node does not have, and a refused grow
  writes no raft entry.
- No shrink leaves sessions over the envelope (unit test of the cap).
- The diurnal simulation shows less stranded capacity than a static
  envelope at the peak, with no more refusals under a smooth load.
