# Checkpoint Work Off the Heartbeat

**Status: SHIPPED** (Phase 1, 2026-10-13): checkpoint requests pushed on
`StreamWork`, checkpoint reports sent on an early heartbeat, the simulated
fleet doing both, and the loadgen numbers in §9. Deliberately deferred: a
dedicated report path that skips the heartbeat's other work (§8, Phase 2,
only if the early beat's cost ever shows up), and pushing the time-based
publish retry (calls #7).

> **Judgement calls:** [`../calls/checkpoints-on-stream-plan_calls.md`](../calls/checkpoints-on-stream-plan_calls.md)

Follows [sandbox-scaling-plan.md](sandbox-scaling-plan.md) §14.6, which
named this as the next latency lever for its wake and idle-resume phases.

## 1. Problem

A placement reaches its node one round trip after it commits, over the
node's `StreamWork` stream (sandbox-scaling plan §6.13). Checkpoint work
does not. Both halves of the checkpoint channel still ride the heartbeat:

- **Requests.** A `capturing`, `deleting`, or unpublished `store` snapshot
  row is work for its node, and the node learns about it only from
  `HeartbeatReply.checkpoint_requests` (`src/manager/server.rs`, the
  heartbeat handler, via `snapshot_rpcs::build_checkpoint_requests`). On
  the default 2 s cadence that is a uniform wait of 0 to 2 s.
- **Reports.** The worker queues each `SnapshotReport` in its outbox
  (`src/worker/checkpoints.rs`) and sends it on its next scheduled beat,
  another wait of up to a full interval.

Measured with `legion-loadgen --mode instances` (sandbox-scaling plan
§14.3, release build, 10 lifecycles/s): suspend p50 2,982 ms, resume p50
2,003 ms, against create → running p50 12.4 ms. A suspend pays a wait for
the request and a wait for the report. A resume's placement is already on
the stream, so it pays only the report's wait, which in that benchmark is
a full interval because the resume is phase-locked to the beat that
finished the suspend.

These are the dominant control-plane latencies for stateful sandboxes that
suspend and resume all the time: idle suspend, wake on request, and fork.
The wake-on-request target (p50 ≤ 1 s from a published manifest,
sandbox-scaling plan §7.2) cannot be met while a resume's outcome waits up
to 2 s to reach the manager, and an operator who stretches the cadence to
relieve a large fleet (`heartbeat.adaptive`) makes every suspend and resume
slower by the same factor.

## 2. Prior art

- **Kubernetes** separates liveness from status. The kubelet renews a
  `Lease` every 10 s for liveness
  ([Leases](https://kubernetes.io/docs/concepts/architecture/leases/)), but
  it watches its pods over a long-lived watch and writes pod status as it
  changes, through its status manager, not on the lease cadence.
- **Nomad** does the same. A client learns its allocations from a blocking
  query on `Node.GetClientAllocs`, and pushes allocation status changes
  through `Node.UpdateAlloc` from a sync loop that batches them over a short
  window (`allocSync` in
  [client/client.go](https://github.com/hashicorp/nomad/blob/main/client/client.go)),
  independent of its heartbeat TTL.
- **Borg** polls each Borglet every few seconds and carries state changes
  on the poll ([Borg, EuroSys 2015](https://research.google/pubs/large-scale-cluster-management-at-google-with-borg/)),
  which is the shape Legion's checkpoint channel still has.

**Take:** the Kubernetes and Nomad split. Work flows down a stream as it
commits, outcomes flow up as they happen, and the periodic beat is for
liveness and reconciliation. **Reject:** a new status RPC per outcome
(Nomad's `UpdateAlloc` shape). Legion's heartbeat already carries
everything a report needs ordered against it (failed allocations,
terminations, health), so sending the heartbeat early is the same thing
with no new surface (calls #2).

## 3. Jobs to be done

**Platform builder** (the person whose product creates and suspends
sandboxes through the API)
- When my user's sandbox goes idle, I want it suspended in about the time
  the checkpoint takes, so its memory is free for the next user right away.
- When a request arrives for a suspended sandbox, I want it running again
  in well under a second, so the first request does not time out.
- When an agent forks a sandbox to explore two branches, I want the forks
  running as soon as the capture finishes, not a few seconds later.

**Cluster operator**
- When I slow the heartbeat down to carry a bigger fleet, I want suspend
  and resume to stay fast, so tuning the control plane does not degrade
  the product.
- When a stream drops or a manager fails over, I want a suspend in flight
  to finish anyway, a little later at worst, never lost.

**On-call responder**
- When a suspend looks stuck, I want the same signals as today (the
  snapshot's state, the heartbeat that carries its request), so nothing
  new has to be learned at 3 a.m.

*Not in scope:* the job author. Nothing in a jobspec changes.

### User scenarios

1. An idle policy suspends 40 sandboxes on one node within a sweep. Each
   capture starts the moment its row commits, and each instance is
   `suspended` as soon as its capture finishes, instead of two beats later.
2. A gateway wakes a suspended sandbox on a request. The restore lands on
   the node over the stream as before, and the manager learns the guest is
   back one round trip after the restore finishes, instead of up to a beat
   later.
3. A worker's stream drops in the middle of a suspend. The push is lost
   with it. The node's next heartbeat carries the request, as it always
   did, and the reconnected stream pushes it again, and the work is done
   once.

## 4. Goals and non-goals

**Goals**
- A suspend's control-plane share is two round trips and two commits, and
  a resume's is a placement and one round trip, at any heartbeat cadence.
- Nothing is lost when a push is: the heartbeat stays the record for
  checkpoint work and the redrive path for reports.
- No new RPC, no new config, no raft entries added to any lifecycle stage
  (`tests/raft_lifecycle_ops.rs` unchanged).

**Non-goals**
- The checkpoint itself (capture, restore, publish mechanics and format),
  owned by other work.
- Edge-boot provisional dispatch on the stream (sandbox-scaling calls #39).
- Making the heartbeat's own work cheaper. An early beat is an ordinary
  beat.

## 5. Background: what exists today

- `src/storage/work_log.rs`: the per-node delivery log behind `StreamWork`.
  The store's changeset projection marks changed allocations
  (`note_work_changes` in `src/storage/store.rs`), the stream task builds a
  message from the store when it sends (`next_work_message` in
  `src/manager/server/work_stream.rs`), and every message is sequenced,
  kept for replay until acked, and folded into a digest the heartbeat
  checks.
- `src/manager/server/snapshot_rpcs.rs`: `build_checkpoint_requests` reads
  a node's outstanding checkpoint work (one indexed lookup on
  `snapshot (node_id, state)`, plus the leader-local warm queue) and builds
  each request, part keys included. The heartbeat handler applies
  `snapshot_reports` first, in one scheduler-lane group commit, then builds
  the reply.
- `src/worker/checkpoints.rs`: `CheckpointState::accept` sorts a reply's
  requests into new work and re-sends, and forgets finished work the reply
  no longer lists. That forgetting is correct only because requests arrive
  on the heartbeat alone (§6.3).
- `src/worker/daemon.rs`: the heartbeat loop already wakes early
  (`HEARTBEAT_WAKE`) for a stream message that stops or changes an
  allocation.
- `src/worker/simulate.rs`: the simulated fleet answers checkpoint
  requests from the heartbeat reply and queues reports for the next beat.

## 6. Design

### 6.1 Requests are pushed on the stream

`StreamWorkReply` gains `repeated CheckpointRequest checkpoint_requests`.
The projection notes every node whose snapshot rows were inserted or
updated (a deleted row is finished work), re-reading the node by key for an
update record that does not name it. WarmSnapshot notes the nodes it queued
warms for, since the warm queue is memory, not a row. The next message for
such a node reads its outstanding checkpoint work with the same
`build_checkpoint_requests` the heartbeat uses and carries the requests
this stream has not been pushed yet.

A push is a hint, not part of the sequenced set:

- **Once per stream.** The log remembers which `(snapshot, op)` it pushed
  and still sees outstanding. A new stream starts empty, so a reconnect is
  pushed everything outstanding.
- **Never replayed.** The copy of a message kept for replay leaves the
  requests out. A replayed push could be minutes stale.
- **No digest.** The digest covers allocations only.
- **The heartbeat is still the record.** The reply keeps carrying the
  node's whole outstanding set every beat. A lost push costs one interval.

A message may carry only pushes. It still takes the next `seq`, so the
worker's mirror sees no gap.

### 6.2 Reports go out on an early heartbeat

When the worker queues a fresh report (a finished capture, publish, warm,
restore, or a local suspension), it wakes its heartbeat loop, which sends
a beat at once. Re-sends never wake. A delete's confirmation does not wake
either: nothing waits on it, and it saves a beat per lifecycle.

The early beat is an ordinary heartbeat, built under the same task-manager
lock as always, so the report and the node's other state (a suspended
allocation's stop, a failed restore's allocation failure) reach the manager
together and in order. Wakes that arrive while a beat is in flight fold
into the next one.

### 6.3 Forgetting finished work needs a delivered report

`accept` used to forget finished work whenever a reply stopped listing it.
With pushes, a reply built before a snapshot row existed can arrive after
the pushed work finished. Forgetting then would make a node redo the work
if the manager re-asks (because the report was lost), and a redone suspend
capture finds its allocation gone and reports a failure for a suspend that
succeeded.

So the worker forgets finished work only once a heartbeat the manager
answered carried its report (`mark_delivered`). The manager applies a
beat's reports before it builds that beat's reply, so the reply that drops
the request after such a beat really is the manager saying it is done.

### 6.4 The simulated fleet does the same

The loadgen's simulated nodes answer pushed requests on arrival, wake their
heartbeat for a fresh report (and when a restore payload is ready), and
apply the same delivered rule. Early beats do not advance their
health-check counter, which a real worker runs on its own timer.

### 6.5 What the user sees

Nothing new to type. The difference is in timings and events:

```console
$ legion instance suspend user-42 --wait
Suspending instance user-42 (memory checkpoint snap-… on w-03)
Instance user-42 suspended (snap-…)        # capture time + a few ms, not + ~3 s
```

## 7. Proto, struct, config, and persistence changes

- `StreamWorkReply.checkpoint_requests` (field 7), documented in
  `proto/legion.proto`. Comments updated on `StreamWork`,
  `CheckpointRequest`, `HeartbeatRequest.snapshot_reports`, and
  `HeartbeatReply.checkpoint_requests`.
- No config, no jobspec field, no migration, no new RPC (so no ACL entry).

## 8. Phases

- **Phase 1 (this change).** §6.1 to §6.4, docs, tests, and numbers.
- **Phase 2 (only on evidence).** If early beats ever show up as manager
  load, give reports a path that skips the rest of the heartbeat (a report
  frame on the stream's upstream half). It would have to carry the
  ordering §6.2 gets for free (calls #2).

## 9. Testing and numbers

- Unit: `work_log` (push once per stream, never replayed, a
  checkpoint-only message keeps the sequence), `checkpoints` (a push starts
  work and forgets nothing, an older reply does not forget unreported
  work), `simulate` (the faked channel answers pushes and wakes).
- Integration: `tests/work_stream.rs`
  `checkpoint_work_reaches_a_streaming_worker_without_a_heartbeat`, on a
  raft manager so the projection runs inside the state machine's apply.
- Regression: `tests/raft_lifecycle_ops.rs` unchanged (pushes and early
  beats write nothing).
- Load: `legion-loadgen --mode instances`, before and after, below.

Before is `origin/main` at `37f43a5` plus the chunked-checkpoint publish
change, after is this change on top. One Apple M-series laptop shared with
other agents' builds (10 cores), release builds, a fresh single-voter raft
manager on isolated ports per run, `--simulate 50`, `--sessions = 20 ×
rate`, `--hold-ms 0`, the default 2 s cadence. Every run completed every
session. Latencies p50 / p99 in ms.

| Offered/s | Build | Completed/s | Create → running | Suspend | Resume |
| --- | --- | --- | --- | --- | --- |
| 10 | before | 7.9 | 6.7 / 64.5 | 2,999 / 4,141 | 1,986 / 2,184 |
| 10 | after | 10.0 | 5.4 / 16.9 | 3.9 / 12.3 | 7.4 / 21.7 |
| 50 | before | 38.2 | 18.1 / 492 | 3,511 / 4,288 | 2,043 / 2,246 |
| 50 | after | 50.0 | 12.6 / 48.7 | 9.6 / 34.8 | 15.3 / 45.8 |
| 100 | before | 72.9 | 380 / 3,726 | 4,806 / 7,043 | 2,536 / 6,737 |
| 100 | after | 96.5 | 66.7 / 407 | 73.6 / 640 | 99.7 / 753 |

Suspend and resume drop from heartbeat-bound seconds to the control plane's
own round trips. Lifecycles per second rise with them, because each session
spends seconds less in flight and the open-loop generator stops queueing
behind them. The simulated capture and restore are instant, so a real
runtime adds its own checkpoint and restore time on top.

## 10. Rollout

On by default, with no knob (calls #3). Workers and managers ship
together (`docs/upgrades.md`). A manager that does not push leaves a new
worker on today's heartbeat timing for requests, and a worker that ignores
pushes leaves the manager's heartbeat as its only source, so either skew
degrades to today's latency, never to lost work.

## 11. Acceptance

- A suspend on a streaming node completes with no scheduled heartbeat in
  between (the integration test asserts the push with no beat at all).
- Loadgen suspend and resume p50 drop from seconds to tens of
  milliseconds at an idle rate, with the lifecycle ceiling and entries per
  session no worse.
- `tests/raft_lifecycle_ops.rs` counts unchanged.
