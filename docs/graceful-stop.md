# Graceful stop

Every allocation Legion removes has a reason, and every task gets warned
before it dies. This page describes the stop ladder — signal, grace window,
SIGKILL — the machine-readable notice a task can read, and where the reason
for a stop shows up afterwards.

One ladder serves **every** platform-initiated termination: `job stop`, a
rollout replacement, a node drain, a reschedule, a `legion job scale` scale-in,
and preemption. There is no second eviction path with different semantics.
That is deliberate: the most-cited operational failure in this problem space
is Kubernetes' ~9 distinct eviction paths with inconsistent grace and budget
behavior, and it is unfixable there precisely because the paths were allowed
to diverge early.

## The ladder

```jsonnet
{
  name: 'web',
  runtime: 'container',
  // How this task is stopped, whoever initiates it. Both fields optional.
  stop: {
    signal: 'SIGTERM',   // default
    killAfter: '30s',    // default; SIGKILL after this grace window
  },
}
```

When the platform stops a task:

1. The worker writes the **stop notice** into the task directory.
2. It sends `stop.signal` to the task's process group.
3. It waits, up to `stop.killAfter`, for the process to exit.
4. If the process is still alive at the deadline, it is SIGKILLed and torn
   down.

Storage is released and the allocation directory removed **after** the process
is gone — never beside the signal. Unmounting a volume out from under a live
task is how a database gets corrupted.

### `stop.signal`

One of `SIGTERM` (default), `SIGINT`, `SIGQUIT`, `SIGHUP`, `SIGUSR1`,
`SIGUSR2`. The signal goes to the task's **process group**, so a
shell-wrapped task's children receive it too.

`SIGKILL` is deliberately not offered. It is the end of the ladder, not a
notice; accepting it as `stop.signal` would let a task opt out of grace
entirely while looking like it had some.

### `stop.killAfter`

Default `30s`, **capped at `5m`**. The cap is an operator ceiling, not an
application setting: grace may slow a drain, it must never wedge one. A spec
asking for more is rejected at submit with the cap named.

A task that exits promptly is reaped promptly — `killAfter` is a deadline, not
a delay. The only thing that waits the full window is a task that ignores its
signal.

## The stop notice

Before signalling, the worker writes a JSON file into the task directory. Its
absolute path is in the task's environment as `LEGION_STOP_NOTICE_PATH`, set
at start:

```json
{
  "cause": "preempted",
  "deadline": "2026-07-28T21:04:05Z",
  "signal": "SIGTERM",
  "graceSeconds": 30,
  "preemptor": { "job": "api", "band": "high" }
}
```

| Field | Meaning |
| --- | --- |
| `cause` | Why the platform is stopping this task — one of [the causes](#the-causes). |
| `deadline` | RFC 3339 instant after which the task is SIGKILLed. |
| `signal` | The signal that accompanied this notice. |
| `graceSeconds` | Seconds between the signal and `deadline`, so a task need not subtract timestamps. |
| `preemptor` | Who took the capacity. Present only when `cause` is `preempted`. |

This is the spot-interruption-notice pattern: the signal is the trigger, the
file carries the *why* and the *when*. A bare SIGTERM is not something a
checkpointing job can branch on; a uniform machine-readable file is. A handler
looks like:

```sh
#!/bin/sh
trap 'handle_stop' TERM
handle_stop() {
  cause=$(jq -r .cause "$LEGION_STOP_NOTICE_PATH")
  case "$cause" in
    preempted|drain) checkpoint_and_exit ;;
    *)               drain_connections_and_exit ;;
  esac
}
```

The notice is written once, immediately before the signal, and left in place —
a task that re-reads it after handling the signal still finds it. Writing it
is **best effort**: a full disk or an unwritable task directory logs a warning
and the stop proceeds. A task must not treat the file's presence as
guaranteed.

## Runtime support

| Runtime | Stop notice delivery |
| --- | --- |
| `exec` | Signals the task's process group directly. Full support. |
| `container` | Signals the container's init, not the launching CLI. On Linux (OCI runtimes) `stop.signal` is delivered exactly. On macOS the Apple `container` runtime owns its own ladder and may SIGKILL on its own schedule if `killAfter` exceeds what it accepts. |
| `sandbox` | **No graceful stop.** A sandbox is a guest OS behind a VMM; signalling the VMM terminates the hypervisor, it does not ask the workload inside to shut down, and Legion has no in-guest agent to relay a notice to. `stop { signal, killAfter }` has no effect, and these allocations are torn down immediately. |
| Out-of-tree plugins | A runtime that tracks a host process inherits the default (signal the process group). One that does not declines the notice, and the worker tears the allocation down immediately — the pre-grace behavior. |

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
| `preempted` | Displaced to make room for higher-priority work ([preemption-and-eviction.md](preemption-and-eviction.md)). Carries a `preemptor` in the stop notice. |
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
   allocation's notice, writes the on-disk notice file, and passes the cause,
   signal and deadline to the task runtime as a `StopContext`. A cause
   describes exactly one stop.
4. **Draining.** The runtime signals and returns *immediately*. The allocation
   moves into the worker's terminating set, and a reaper on the supervisor
   loop (once a second) finishes it — on process exit, or at the deadline.
   The stop never blocks the heartbeat path: a `stop()` that waited for a
   30-second deadline would hold the task-manager mutex that heartbeats, the
   self-fence watchdog and health reporting all need, and at a 2-second
   cadence the manager would declare the node lost and reschedule everything
   on it.
5. **Ordering.** Removals are applied before starts, and while anything on a
   node is still inside its grace window no *new* allocation starts there.
   This is what makes plan-carried eviction safe physically: the manager's
   plan is atomic logically, and the worker serializes the physical handover,
   so a node is never running victim and preemptor past the grace window. A
   new allocation landing on a node with slow-stopping neighbours inherits
   their grace as start latency, bounded by the 5-minute `killAfter` cap.
6. **Expiry.** Notices older than 10 minutes are deleted. The delete rides the
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

- **`sandbox` tasks get no grace.** See [Runtime support](#runtime-support).
  The limitation is structural (no in-guest agent), not an oversight.
- **The Apple `container` runtime owns its own ladder.** `stop.signal` is
  passed through, but the runtime's own timeout may fire before Legion's
  `killAfter`. Linux/OCI workers are exact.
- **The grace window costs stop latency for everyone.** Stops that used to be
  instantaneous now take up to `killAfter`. This is the intended behavior
  change and it applies to jobs that declare no `stop` block: the default
  ladder is `SIGTERM` + `30s`, not a hard kill. Set `stop: { killAfter: '0s' }`
  on a task that genuinely wants the old behavior.
- **Starts wait behind co-located stops.** A new allocation landing on a node
  with in-grace allocations waits for them, even when it is unrelated to
  whatever is stopping. The `killAfter` cap bounds the wait; the invariant it
  buys — "a node never runs victim and preemptor past the grace window" — is
  the one an operator can rely on.
- **The deadline in the `allocation.stopping` event is the manager's
  estimate.** The worker's real window opens when it delivers the signal, up
  to one heartbeat later.
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
## See also

- [Preemption and eviction](preemption-and-eviction.md) — priority bands and
  the one cause that carries a preemptor link.

- [Event stream](event-stream.md) — consuming `allocation.removed` and the
  rest of the journal.
- [Architecture: managers and workers](architecture-managers-workers.md) —
  the heartbeat loop that carries notices.
- [Task runtime plugins](task-runtime-plugins.md) — where `StopContext`
  reaches a runtime, and the plugin ABI version that guards it.
