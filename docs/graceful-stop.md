# Stop causes

Every allocation Legion removes has a reason. This page describes how that
reason is recorded, how it reaches the worker running the task, and where you
can see it.

> **Scope.** This page documents what ships today: the *cause* of a stop is
> recorded and delivered. Tasks are still terminated immediately — there is no
> grace period, no configurable signal, and no on-disk notice a task can read.
> Those are a separate, planned change, and this page is where they will be
> documented when they land (hence the file name); until then, see
> [Known limitations](#known-limitations) for exactly what is missing.

## Why this exists

A worker learns that an allocation should stop by noticing it is **absent**
from the desired set in its heartbeat reply. An absence carries no reason, so
a node drain, a rollout replacement, a `job stop`, and a scheduler-driven
reschedule all looked identical — to the worker, to its logs, and to anyone
reading the event journal afterwards. "Why did my task go away?" had no
answer short of correlating timestamps by hand.

Stop causes close that gap. The manager records why, in the same transaction
that removes the allocation, and serves it to the owning node.

## The causes

| Cause | Meaning |
| --- | --- |
| `stop` | The job, or a keyed instance of it, was stopped by a user. |
| `update` | Replaced by a newer job config — a rollout or an in-place update. |
| `drain` | The node is draining and its work is moving elsewhere. |
| `reschedule` | Moved by the scheduler: failure recovery, `onLost` handling, or a scheduler-driven move. |
| `destroy` | The job was destroyed outright. |
| `node_lost` | The node was removed or declared lost, clearing its allocations. |
| `scale_in` | Surplus after `legion job scale` lowered the task's count ([autoscaling.md](autoscaling.md)). Distinct from `update`: no new job config version is involved. |
| `preempted` | Displaced to make room for higher-priority work. **Reserved** — nothing emits this today; it lands with preemption. |
| `unknown` | No cause was recorded. See [Delivery guarantees](#delivery-guarantees). |

`unknown` is a normal value, not only an error case. The most common source is
the worker-failure-report path: a task that died on its own was never stopped
by the platform, so there is no platform stop to explain.

## Where you can see it

**In the event journal.** `allocation.removed` events carry the cause in their
metadata:

```console
$ legion events list --kind allocation.removed -j
{
  "ok": true,
  "data": {
    "events": [
      {
        "kind": "allocation.removed",
        "level": "info",
        "jobId": "batch-transcode",
        "message": "Removed 3 allocation(s) for job batch-transcode",
        "metadata": { "count": 3, "cause": "stop" }
      }
    ]
  }
}
```

The `cause` key is omitted rather than set to `"unknown"` when no cause was
recorded, so a filter on `cause` never matches noise.

The event journal is bounded (roughly 10k events), so this is an
observability surface, not an audit log. For long-term records, consume
[the event stream](event-stream.md) into your own store.

**In worker logs.** The worker logs one line per stop at `info`, naming the
cause:

```
INFO Stopping allocation allocation_id=412 task_name=web cause=drain
```

## How it works

1. **Recording.** Every allocation-removal path in the manager writes a row to
   `allocation_stop_notice` **in the same transaction** as the `DELETE` it
   describes. A notice therefore can never claim a stop that was rolled back,
   and can never arrive describing a stop that has not been decided yet.
2. **Delivery.** Each heartbeat reply carries `stop_notices` — every live
   notice for that node, as a snapshot rather than a delta. The worker
   replaces its copy wholesale.
3. **Consumption.** When the worker stops an allocation, it consumes that
   allocation's notice and passes the cause to the task runtime as a
   `StopContext`. A cause describes exactly one stop.
4. **Expiry.** Notices older than 10 minutes are deleted. The delete rides the
   next removal's transaction rather than the heartbeat path, so reading
   notices never writes, and a cluster doing no removals is not accumulating
   rows to prune.

### Delivery guarantees

Stop-cause delivery is **best effort and advisory**. Specifically:

- **A stop is never delayed or skipped for want of a cause.** If the notice
  has not arrived, the worker stops the allocation with `unknown`.
- **A worker that reconciles before the notice reaches it** records `unknown`.
  In practice the notice is written before the allocation leaves the desired
  set, so it is available on the same heartbeat, but this is not guaranteed
  and nothing depends on it.
- **A worker talking to a manager older than this feature** receives no
  notices and records `unknown` throughout. Nothing else changes.
- **Notices expire after 10 minutes.** A worker partitioned for longer than
  that reconnects and stops its allocations with `unknown`.
- **Unrecognized causes degrade to `unknown`.** A newer manager may send a
  cause an older worker has never heard of; the worker stops the allocation
  anyway. This is what lets causes be added without a coordinated upgrade.

## Interaction with adjacent features

- **[Node drain](architecture-managers-workers.md)** — drained allocations
  carry `drain`; stateful allocations held back by the `onLost` gate are not
  removed at all, so they produce no notice.
- **[Rollouts](rollouts.md)** — allocations replaced by a rollout carry
  `update`, distinguishing a deliberate replacement from a failure.
- **[Stateful workloads](stateful-workloads.md)** — stopping a keyed instance
  carries `stop`; destroying one carries `destroy`.
- **[Raft/replicated managers](multi-raft.md)** — `allocation_stop_notice` is
  owned by the scheduler lane, the same lane as every removal that writes it.
  Notices replicate with the removals they describe.

## Known limitations

- **Termination is still immediate.** The cause is delivered, but no task gets
  a grace period. There is no `stop { signal, killAfter }` job-config block,
  no SIGTERM→SIGKILL ladder, and no `LEGION_STOP_NOTICE_PATH` file for a task
  to read. A task still cannot flush, checkpoint, or drain connections before
  it is killed. Adding that is a separate change, planned but not shipped —
  the `StopContext` handed to task runtimes exists to carry the signal and
  deadline when it lands.
- **No CLI filter by cause.** `legion events list --kind allocation.removed`
  returns all removals; filter on the `cause` metadata key client-side.
- **Causes are per-removal-batch in the journal, per-allocation on the wire.**
  A `job stop` that removes 12 allocations produces one `allocation.removed`
  event with `count: 12`, and 12 individual notices.
- **Allocation IDs are reused.** An allocation ID freed by a deletion can be
  assigned to a new allocation. A stale notice is overwritten when the new
  allocation is itself removed, so a mislabel would require a worker to stop
  an allocation the manager did not remove — which does not happen. The
  10-minute expiry bounds the window regardless.
- **`preempted` is defined but unused.** It is in the vocabulary so that
  workers deployed today already understand it when preemption ships.

## See also

- [Event stream](event-stream.md) — consuming `allocation.removed` and the
  rest of the journal.
- [Architecture: managers and workers](architecture-managers-workers.md) —
  the heartbeat loop that carries notices.
- [Task runtime plugins](task-runtime-plugins.md) — where `StopContext`
  reaches a runtime.
