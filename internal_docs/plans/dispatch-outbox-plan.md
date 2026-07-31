# Durable Dispatch Outbox (manager → worker command delivery)

**Status: PLANNED** — nothing here has shipped. Phase 0 is a measurement
phase and is explicitly allowed to kill the later phases. Deliberately
deferred from the outset: pre-commit / speculative dispatch (that is
[`edge-boot-plan.md`](edge-boot-plan.md)), cross-manager delivery
fan-out, and any change to the at-most-one-live-allocation invariant.

> **Judgement calls:** [`../calls/dispatch-outbox-plan_calls.md`](../calls/dispatch-outbox-plan_calls.md)

## Problem

Legion has two ways to get an allocation onto a worker, and neither one is
durable.

**Path 1 — best-effort push.** After a plan commits, `dispatch_allocations`
([`src/manager/dispatcher.rs:11`](../../src/manager/dispatcher.rs)) walks
the placed allocations *sequentially*, builds a `TaskAllocation` per
allocation, and issues one unary `PostWork` RPC per allocation over a
lazily-connected channel per worker address. It is called post-commit from
the eval path ([`src/structs/eval.rs:309`](../../src/structs/eval.rs)) and
from three sites in the rollout path
([`src/manager/rollout.rs:928`](../../src/manager/rollout.rs), `:960`,
`:1460`).

When the RPC fails, the handler logs a warning and moves on
(`dispatcher.rs:77-85`). The allocation is dropped. Nothing records that
delivery was attempted, nothing retries, and no metric moves — the only
trace is one `warn!` line, sitting next to one `info!` line per successful
dispatch (`dispatcher.rs:86`).

**Path 2 — desired-state reconcile on the heartbeat.** Every worker
heartbeats on a jittered interval defaulting to 2s
([`src/constants.rs:68`](../../src/constants.rs),
[`src/worker/daemon.rs:476`](../../src/worker/daemon.rs)), and the reply
carries `desired_allocations`: the worker's **entire** desired set,
rebuilt from the store on every beat
([`src/manager/server.rs:4990`](../../src/manager/server.rs), builder at
`:7455`), alongside `upstream_endpoints` for every declared upstream. The
worker reconciles to that set, learning that an allocation should stop by
noticing its *absence* (`proto/legion.proto:2583`).

That reconcile is what makes path 1's dropped allocations eventually
correct. It is also what they cost:

- **The latency floor is the tick.** A dropped or raced dispatch is not
  noticed until the next heartbeat, so its start latency is
  *up to the full heartbeat interval plus reconcile* — and because nothing
  counts the drop, that tail is invisible in every number we publish. This
  is the same reconciliation-tick floor the prior art below was built to
  remove.
- **Steady-state cost is proportional to the fleet, not to change.** A node
  running 200 allocations receives 200 fully-serialized `TaskAllocation`
  payloads every 2s forever — each carrying task config, checks,
  `on_unhealthy`, artifacts, connect, volumes, identities, resources —
  whether or not anything changed. The manager pays
  `build_desired_allocations` + `build_upstream_endpoints` per heartbeat,
  per node.
- **Delivery is unobservable.** There is no backlog gauge, no drain rate,
  no delivery lag, no ack. We cannot answer "is the control plane behind?"
  except by inference from allocation states.
- **The two paths can duplicate and reorder.** Push and reconcile may both
  deliver the same allocation, in either order. It is safe only because
  desired-state application is idempotent — which is exactly the property
  that forces the full set onto the wire every time.

The [heartbeat-scale plan](heartbeat-scale-plan.md) already removed the
per-heartbeat waste that no architecture would forgive (5.3–6.7x on raft,
~2x local-disk — [`benchmarks/scheduler/README.md`](../../benchmarks/scheduler/README.md)).
What it deliberately did *not* touch is the shape of the payload: the
heartbeat still carries the world. This plan is about that.

## Prior art

- **Nomad.** Clients receive allocation updates through *blocking queries*
  on their node's alloc list: the client long-polls with the last index it
  saw, the server holds the request open until the index moves, and the
  reply carries a diff keyed by that index. Delivery latency is one RTT
  after the write, not one tick, and steady state costs a parked
  connection rather than a repeated payload.
- **Kubernetes.** The kubelet watches its node's pods via an informer over
  a long-lived stream keyed by `resourceVersion`. Deltas on the wire, full
  relist only on a version gap the server can no longer serve — the same
  "resync when the cursor is stale" escape hatch.
- **Tensorlake.** Publishes the shape most directly:
  scheduling decisions write an `AddSandbox` command to a durable RocksDB
  outbox *in the same transaction as the decision*; dataplanes long-poll
  over gRPC and ack the highest sequence number they processed; the
  scheduler deletes only acked commands, and a sequence-number regression
  triggers a full state resync. Their reported delivery measurement is
  1,000 commands drained in 340ms across four hosts, with backlog and
  drain-rate exposed as first-class gauges.

**What we take:** the durable command written in the decision's own
transaction, per-node sequence numbers, ack-on-next-poll, long-poll
delivery, and resync-on-regression.

**What we reject:** replacing desired-state reconcile with commands
outright. Reconcile is what makes Legion self-healing against every bug we
have not thought of, and losing it to buy latency is a bad trade. Commands
become the *fast* path; reconcile stays the *recovery* path and stops
running every 2s. This is judgement call
[#1](../calls/dispatch-outbox-plan_calls.md).

## Jobs to be done

**Cluster operator**
- When I am deciding whether the control plane is keeping up, I want a
  number that says how far behind delivery is, so I can tell "slow to
  start" from "not started at all" without reading logs on two machines.
- When a manager restarts mid-deploy, I want the work it had already
  decided on to still happen, so a restart is not a silent partial
  rollout.

**Job author / app developer**
- When I ship a change, I want the time between "the manager accepted it"
  and "the task is running" to be predictable, so a slow start tells me
  something about my image and not about the scheduler's clock.

**On-call responder**
- When tasks are not starting, I want to see whether the manager decided
  and failed to deliver, or never decided, so I know which half of the
  system to look at.

**Autonomous agent**
- When I start a task and poll for it, I want the delay to be bounded by
  the network rather than a tick I cannot see, so my own timeouts do not
  have to assume the worst case.

*Not in scope:* the security/platform admin persona — this changes how an
already-authorized decision is delivered, not who may make it. The
worker-plane ACL treatment is unchanged (`acl::enforce_worker_plane`).

### User scenarios

1. **A burst of 1,000 placements.** An operator submits a job scaling to
   1,000 allocations across a warm fleet. Today: 1,000 sequential unary
   RPCs from one thread, and every failure waits for a 2s tick. After:
   1,000 commands land in the apply transaction, every affected node's
   parked poll wakes within an RTT, and the operator can watch the backlog
   gauge drain.
2. **The manager restarts between decide and deliver.** Today the
   allocations exist in the store and the workers find out on their next
   heartbeat. After, the commands are in the store too, and delivery
   resumes from the last acked sequence with no tick involved.
3. **A worker is partitioned for 90 seconds.** Its poll fails; commands
   accumulate against its node with a visible backlog. When it returns, it
   polls with its last sequence, gets everything it missed in one batch,
   and — if the backlog was truncated or its sequence is unrecognized —
   is told to resync and falls back to today's full desired-state path.

## Goals

- Bound delivery latency by network RTT rather than the heartbeat
  interval, and make the *failure* to deliver a counted event.
- Make delivery durable: a decision that commits is a command that
  survives a manager restart.
- Make steady-state per-node control traffic proportional to *change*
  rather than to the number of allocations on the node.
- Expose backlog, drain rate, and delivery lag as first-class metrics.
- Produce a defensible, honestly-scoped delivery throughput number
  (see [Measurement](#measurement-what-number-this-produces)).

## Non-goals

- Pre-commit or speculative dispatch — [`edge-boot-plan.md`](edge-boot-plan.md)
  owns starting work before the placement is durable.
- Removing the desired-state reconcile path. It stays, gated to run on
  resync rather than on every beat.
- Changing planning, placement, or the epoch CAS. This plan starts after
  the plan commits.
- Multi-manager delivery fan-out. One leader delivers, as today.
- Making `PostWork` disappear. It remains as an optional low-latency
  nudge and as the v2-worker compatibility path.

## Background — what exists today

| Piece | Where | Behavior |
| --- | --- | --- |
| Push dispatch | `src/manager/dispatcher.rs:11` | Sequential unary `PostWork` per allocation; failure → `warn!` + `continue`, allocation dropped |
| Payload builder | `src/manager/dispatcher.rs:124` | `build_task_allocation` — shared with the heartbeat path |
| Dispatch call sites | `src/structs/eval.rs:309`; `src/manager/rollout.rs:928,960,1460` | All strictly post-commit |
| Desired-state reply | `src/manager/server.rs:4990`, builder `:7455` | Full set rebuilt per heartbeat |
| Heartbeat cadence | `src/constants.rs:68`, `src/worker/daemon.rs:476,555` | 2s default, jittered, `heartbeatSeconds` configurable |
| Wire contract | `proto/legion.proto:2577` | `HeartbeatReply.desired_allocations`, absence means stop |
| Worker reconcile | `src/worker/simulate.rs:252` (sim), worker daemon equivalent | Applies the desired set idempotently |
| Fused plan apply | `Store::submit_and_apply_plan` | Plan insert + apply already commit as ONE replicated op |
| Group commit | D5, shipped 2026-07-30 | Concurrent applies share one quorum round |

Two facts from that table carry the design. First, `submit_and_apply_plan`
already exists as a single fused replicated op — so writing commands "in
the same transaction as the decision" is a third statement in an existing
transaction, not a new commit. Second, group commit means that added write
is amortized across concurrent applies rather than paid per plan.

## Proposed design

### 1. The outbox table

A `allocation_command` table on the **scheduler lane**, written inside
`submit_and_apply_plan`:

| Column | Notes |
| --- | --- |
| `node_id` | Delivery target; the sequence is per node |
| `seq` | Monotonic **per node**, allocated inside the write |
| `kind` | `start` \| `stop` \| `update` |
| `allocation_id` | The allocation this concerns |
| `payload` | Serialized `TaskAllocation` for `start`/`update`; empty for `stop` |
| `created_at` | Sortable, for delivery-lag measurement |

Commands are deleted when acked (§3), so the table is a queue, not a log.
A command is the direct output of the decision — never re-derived from a
state diff — which is what makes "the manager crashed after deciding" a
non-event.

### 2. The poll RPC

```protobuf
// Worker-plane. Long-polled: the manager holds the request open until a
// command is available for this node or the hold expires.
rpc PollWork (PollWorkRequest) returns (PollWorkReply);
```

`PollWorkRequest` carries `node_id` and `last_seq` (the highest sequence
the worker has durably applied). `PollWorkReply` carries a batch of
commands, the highest sequence in the batch, and a `resync_required` flag.

The manager parks the request on a per-node `Notify` woken at commit, so
delivery is one RTT after the write with no polling interval anywhere in
the path. Hold duration and batch ceiling are config (§5).

### 3. Acks, and what happens when they go wrong

The next poll's `last_seq` **is** the ack: the manager deletes commands at
or below it for that node. There is no separate ack RPC and no ack in the
heartbeat.

If `last_seq` is ahead of what the manager has, or behind the oldest
command it still holds, the manager sets `resync_required` and the worker
falls back to the desired-state path — the manager sends the full
`desired_allocations` set on the next heartbeat, exactly as it does today,
and the worker reconciles to it. Sequence regression is a counted event
(`legion_dispatch_outbox_resync_total`), not a silent correction.

This is the whole safety argument: **the outbox can be wrong, lossy, or
skipped entirely and the cluster still converges**, because the path that
converges it today is still there. The outbox buys latency and
observability; reconcile keeps buying correctness.

### 4. Heartbeat sheds the world

Once a worker is polling and caught up (protocol version ≥ 3), its
heartbeat reply carries `desired_allocations` **only** when
`resync_required` is set. Liveness, health reports, volume reports, and
upstream endpoints are unaffected. A v2 worker keeps receiving the full
set on every beat, forever — mixed fleets are a supported steady state,
not a migration window (see [Rollout](#rollout-and-backward-compatibility)).

### 5. Surface

Manager config:

```jsonnet
{
  dispatch: {
    // 'push'   — today's behavior: best-effort PostWork, full desired
    //            state every heartbeat. The default until measured.
    // 'outbox' — commands are the delivery path; heartbeat sheds
    //            desired state for workers at protocol >= 3.
    // 'auto'   — outbox for workers that support it, push for the rest.
    mode: 'push',
    pollHoldSeconds: '30s',   // how long a parked poll is held open
    batchMax: 128,            // commands per poll reply
    retainAcked: '0s',        // >0 keeps acked commands for debugging
  },
}
```

Operator-visible state:

```console
$ legion node dispatch node-7 -j
{"ok":true,"data":{"node_id":"node-7","last_acked_seq":41822,
 "pending":3,"oldest_pending_age_ms":12,"polling":true,"resyncs_total":0}}
```

### 6. Metrics

- `legion_dispatch_outbox_backlog{node}` — undelivered commands (gauge).
- `legion_dispatch_outbox_delivery_seconds` — commit → poll reply
  (histogram). **This is the delivery latency number.**
- `legion_dispatch_outbox_drain_total` — commands delivered (counter).
- `legion_dispatch_outbox_resync_total{reason}` — regression, truncation,
  unknown node.
- `legion_dispatch_push_dropped_total` — the failure today's code only
  logs. Landing this counter in Phase 0 is worth the phase on its own.

## Proto, struct, config, and persistence changes

- **Proto**: new `PollWork` RPC + `PollWorkRequest`/`PollWorkReply`, both
  fully commented per the documentation rule. `HeartbeatReply` gains no
  fields; its `desired_allocations` becomes conditionally empty, which the
  field comment must state precisely (an empty set already means "stop
  everything", so the *condition* is load-bearing and needs a companion
  `desired_allocations_present` bool to keep the two cases distinguishable
  on the wire — see calls [#4](../calls/dispatch-outbox-plan_calls.md)).
- **Store**: `allocation_command` table + migration; `submit_and_apply_plan`
  gains the command write; new `poll_commands` / `ack_commands` helpers.
- **Config**: `dispatch` block above, in `configs/manager.jsonnet` and
  documented. No job-config fields, so `configs/jobs/maximal.jsonnet` is
  untouched by this plan.
- **ACL**: `PollWork` is worker-plane — `acl::enforce_worker_plane`, plus
  an entry in `tests/acl_endpoints.rs` (`endpoint_table` + request
  builder), which is not optional.

## Implementation phases

**Phase 0 — measure the path we have.** A `tests/dispatch_scale_benchmark.rs`
in the style of the heartbeat benchmark: N nodes, a burst of M placements,
measuring commit→worker-observes latency (p50/p99), sustained
commands/sec, and the push drop rate under induced worker failures. Ship
`legion_dispatch_push_dropped_total` with it. **This phase may kill the
plan**: if drops are ~0 and tick-floor latency does not show up in the
tail at fleet scale, the rest is not worth the wire surface.

**Phase 1 — the outbox, in shadow.** Table, migration, command write in
`submit_and_apply_plan`, backlog/drain metrics. Push stays authoritative
and nothing reads the outbox. Verifiable on its own: commands written
match dispatches 1:1, and the plan-apply benchmark shows the write's cost.

**Phase 2 — delivery.** `PollWork` + worker poll loop; outbox becomes the
delivery path under `dispatch.mode: 'outbox'`. `PostWork` is retained as
an optional nudge. Heartbeat unchanged — still sending the full set, so
this phase is safe to run with the reconcile backstop fully active.

**Phase 3 — shed the world.** Protocol version 3; heartbeat sends
`desired_allocations` only on resync. This is the phase that wins the
steady-state cost, and it is last because it is the one that removes the
safety net's redundancy.

**Phase 4 — publish.** Re-run Phase 0's benchmark, record before/after in
`benchmarks/scheduler/README.md` with host, build profile, and machine
state, and write the public page.

Each phase is independently valuable: 0 gives us a drop counter we do not
have, 1 gives durability, 2 gives latency, 3 gives steady-state cost.

## Testing plan

- **Unit**: per-node sequence monotonicity under concurrent applies; ack
  truncation deletes exactly `<= last_seq` for that node only; regression
  and truncation both set `resync_required`.
- **Property — the one that matters**: for a random sequence of plan
  applies and induced delivery failures, the allocation set a worker
  converges to via the outbox path is **identical** to the set it
  converges to via desired-state reconcile alone. Modeled on the
  scheduling-index equivalence property; this is the test that lets us
  keep both paths without them drifting.
- **Integration**: manager killed between apply and delivery (commands
  survive, delivery resumes); worker restart with lost cursor (resync
  fires, converges); partitioned worker for > `pollHoldSeconds`; a v2 and
  a v3 worker on the same manager, both converging.
- **Regression**: existing dispatch and heartbeat suites unchanged under
  `dispatch.mode: 'push'` — the default — so Phases 1–2 cannot regress
  anyone who has not opted in.
- **ACL**: `cargo test --test acl_endpoints` for the new RPC.
- **Benchmark**: Phase 0's harness, re-run per phase.

## Rollout and backward compatibility

- `dispatch.mode` defaults to `push` — today's behavior byte for byte —
  through Phases 1–3. `auto` becomes the default only after the Phase 4
  numbers exist, and `push` remains selectable indefinitely.
- Worker protocol version gates Phase 3. A v2 worker against a v4 manager
  gets today's semantics forever; there is no flag day and no minimum
  worker version. Per [`docs/upgrades.md`](../../docs/upgrades.md), the
  manager upgrades first.
- The migration adds a table and no columns to existing rows, so
  downgrade is "stop reading it" — the table is orphaned, not corrupting.
- If the outbox misbehaves in production, `dispatch.mode: 'push'` is a
  config change, not a rollback: reconcile is still running underneath.

## Measurement — what number this produces

This plan exists partly because the industry number that prompted it —
"more than 24,000 sandboxes per second" — turns out on inspection to be a
*scheduling-decision* rate published without its delivery or start rate,
in a post with no methodology, while the same vendor's one concrete
delivery measurement (1,000 commands in 340ms across four hosts, ≈2,900/s)
sits an order of magnitude below it in a different post. We are not going
to reproduce that.

Three metrics, always reported together, never collapsed into one:

1. **`commands_committed_per_sec`** — decisions durably written. Bounded
   by the store's commit rate; the number most flattering to us and the
   least meaningful on its own.
2. **`commands_delivered_per_sec`** — commands a worker has acked.
   Bounded by delivery, and the number this plan is actually about.
3. **`allocations_running_per_sec`** — end-to-end, worker reports the task
   running. Bounded by the worker's runtime, and the only one a user
   experiences.

Every published figure carries: fleet size, backend (`local_disk` /
`raft1` / `raft3` / `streaming`), host and build profile, driver
concurrency, correctness (expected vs actual allocations), and the resync
rate. A run with a nonzero resync rate is a run where delivery partially
failed and reconcile covered for it — reporting throughput from that run
without the resync count would be a lie of omission.

**What we will claim:** a delivery rate, at a stated fleet size and
backend, with the end-to-end start rate next to it.
**What we will not claim:** a decision rate as a start rate; any number
from a single laptop run; any number without its correctness column.

## Open questions

1. **Should `stop` commands carry a reason?** Today absence-means-stop
   carries no reason, which the proto comment already calls out as a
   deficiency (`proto/legion.proto:2614`). An explicit `stop` command is
   the natural place to fix it. *Lean: yes, but in a follow-up* — it is a
   worker-visible semantic change and does not need to ride this plan.
2. **Per-node `Notify` at 10k nodes.** 10k parked polls means 10k held
   HTTP/2 streams. Nomad does exactly this, so it is known-survivable, but
   it interacts with our connection limits. *Lean: measure in Phase 0 with
   the simulated fleet before committing to hold duration defaults.*
3. **Does the command write belong on the scheduler lane or the status
   lane?** Scheduler lane keeps it in the apply transaction (the point);
   status lane keeps delivery churn off the scheduling critical path.
   *Lean: scheduler lane — transactional coupling is the feature. Revisit
   if lane latency moves in the Phase 1 benchmark.*
4. **Retention for debugging.** `retainAcked` above is speculative
   surface. *Lean: ship it at `0s` and only keep the knob if Phase 2
   debugging actually wants it.*

## Acceptance criteria

- **Phase 0**: a committed benchmark that reports commit→observe p50/p99
  and a nonzero-when-induced push drop counter; a before-table in
  `benchmarks/scheduler/README.md`.
- **Phase 1**: crash between apply and delivery loses no command
  (integration test); command-write cost visible and bounded in the
  plan-apply benchmark.
- **Phase 2**: with `mode: 'outbox'`, commit→observe p99 is bounded by RTT
  and independent of `heartbeatSeconds` — demonstrated by tripling the
  heartbeat interval and showing the delivery histogram unmoved.
- **Phase 3**: steady-state heartbeat reply bytes per node are independent
  of allocations-per-node (measured at 1, 50, 200 allocations/node); the
  equivalence property passes.
- **Phase 4**: `benchmarks/scheduler/README.md` carries the before/after
  with all three rates from the Measurement section, and a public page in
  `docs/` documents the mode, the resync semantics, and the failure modes.
