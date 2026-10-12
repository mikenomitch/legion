# Graceful stop

When Legion removes an allocation, it records why, and it warns the task
before killing it. This page covers the stop ladder (signal, grace window,
SIGKILL), the machine-readable notice a task can read, and where you can see
the reason for a stop afterward.

The same ladder handles **every** stop the platform starts: `job stop`, a
rollout replacement, a node drain, a reschedule, a `legion job scale`
scale-in, and preemption. There is no second eviction path with different
behavior. In Kubernetes, by comparison, [API-initiated
eviction](https://kubernetes.io/docs/concepts/scheduling-eviction/api-eviction/)
respects PodDisruptionBudgets, while [node-pressure
eviction](https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/)
ignores them and can skip the pod's grace period. Legion keeps one path so
every stop behaves the same way.

## The ladder

```nickel
{
  name = "web",
  runtime = "container",
  # How this task is stopped, whoever initiates it. Both fields optional.
  stop = {
    signal = "SIGTERM",   # default
    killAfter = "30s",    # default; SIGKILL after this grace window
    drain = "5s",         # service tasks only; default one heartbeat interval
  },
}
```

When the platform stops a task:

0. **If the task advertises a `service`**, it first leaves the catalog and
   keeps serving for the **drain window** (`stop.drain`, or one heartbeat
   interval of its node). This gives consumers on other nodes time to learn
   it is gone before it stops accepting connections.
1. The worker writes the **stop notice** into the task directory.
2. It sends `stop.signal` to the task's process group.
3. It waits up to `stop.killAfter` for the process to exit.
4. If the process is still alive at the deadline, its whole process group is
   SIGKILLed and torn down. The kill reaches the same group the signal did, so
   a shell-wrapped task (`sh -c server`) can't leave the server running after
   its shell is gone.

Storage is released and the allocation directory removed only **after** the
process is gone, never at the same time as the signal. Unmounting a volume
under a live task can corrupt a database.

### `stop.signal`

One of `SIGTERM` (default), `SIGINT`, `SIGQUIT`, `SIGHUP`, `SIGUSR1`,
`SIGUSR2`. The signal goes to the task's **process group**, so the children
of a shell-wrapped task receive it too.

`SIGKILL` is not allowed. It is the last step of the ladder, not a warning.
Accepting it as `stop.signal` would let a task skip the grace period while
appearing to have one.

### `stop.killAfter`

Default `30s`, **capped at `5m`**. The cap is an operator limit. Grace can
slow a drain, but it must never block one. A spec that asks for more is
rejected at submit with an error that names the cap.

`killAfter` is a deadline, not a delay. A task that exits quickly is cleaned
up quickly. Only a task that ignores its signal waits the full window.

### `stop.drain`

Applies only to tasks with a [`service`](service-discovery.md) block. Other
tasks ignore it, because nothing routes to a task that advertises nothing.

Without a drain, rollouts cause `connection refused` errors. The manager
removes a stopping allocation from each node's endpoint feed on that node's
next heartbeat. But the node running the allocation also learns to stop it
on its own next heartbeat. So a consumer on another node can be routed to an
endpoint that has already received `SIGTERM`. The drain window fixes the
order. The allocation leaves the live set and the catalog first, keeps
serving, and gets its signal only after the window, when every other node has
heard.

- **Default: one heartbeat interval** of the node running the task (its
  actual cadence, including one set by the manager). That is how long the
  rest of the cluster needs.
- **`"0s"` disables it** for a task whose consumers retry anyway.
- **Capped at `5m`**, like `killAfter`, for the same reason.
- An in-place re-dispatch of the same allocation (an artifact-only update)
  never drains. The allocation relaunches right away and needs its ports.
- A draining allocation still holds its ports and its process, so new
  allocations on the node wait until it is gone, the same as for an
  allocation in its grace window.
- The window is a time limit, not an acknowledgment. A node whose heartbeat
  is late learns late, and a connection it opens in that gap is still
  refused.

`legion job status` shows the allocation as gone as soon as the drain starts.
The worker logs `Draining allocation before stop` with the window, then the
usual `Stopping allocation` line when it sends the signal.

## The stop notice

Before sending the signal, the worker writes a JSON file into the task
directory. The task's environment has its absolute path in
`LEGION_STOP_NOTICE_PATH`, set at start:

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
| `cause` | Why the platform is stopping this task. One of [the causes](#the-causes). |
| `deadline` | RFC 3339 time after which the task is SIGKILLed. |
| `signal` | The signal sent with this notice. |
| `graceSeconds` | Seconds between the signal and `deadline`, so a task does not need to subtract timestamps. |
| `preemptor` | Who took the capacity. Present only when `cause` is `preempted`. |

This follows the cloud spot-interruption notice pattern. The signal is the
trigger, and the file says why and when. A job that checkpoints can't decide
what to do from a bare SIGTERM, but it can from this file. A handler looks
like this:

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

The notice is written once, right before the signal, and left in place. A
task that reads it again after handling the signal still finds it. Writing
it is **best effort**. If the disk is full or the task directory is not
writable, the worker logs a warning and the stop continues. Don't assume the
file is always there.

## Runtime support

| Runtime | Stop notice delivery |
| --- | --- |
| `exec` | Signals the task's process group directly. Full support. |
| `container` | Signals the container's init, not the launching CLI. On Linux (OCI runtimes), `stop.signal` is delivered as configured. When the jobspec has no `stop` block, the image's own `STOPSIGNAL` is used before falling back to `SIGTERM`, matching Docker's behavior (see [task-runtime-container.md](task-runtime-container.md#graceful-stop)). On macOS, the Apple `container` runtime runs its own ladder and may SIGKILL on its own schedule if `killAfter` exceeds what it accepts. |
| `sandbox` | **No graceful stop.** A sandbox is a guest OS behind a VMM. Signaling the VMM terminates the hypervisor instead of asking the workload inside to shut down, and Legion has no in-guest agent to pass a notice to. `stop { signal, killAfter }` has no effect, and these allocations are torn down immediately. |
| Out-of-tree plugins | A runtime that tracks a host process gets the default (signal the process group). A runtime that does not declines the notice, and the worker tears the allocation down immediately, as it did before graceful stop existed. |

## Why this exists

A worker learns that an allocation should stop when the allocation is
**missing** from the desired set in its heartbeat reply. A missing entry has
no reason attached. A node drain, a rollout replacement, a `job stop`, and a
scheduler reschedule all looked the same to the worker, in its logs, and in
the event journal. The only way to answer "why did my task go away?" was to
match up timestamps by hand.

Stop causes fill that gap. The manager records the reason in the same
transaction that removes the allocation, and sends it to the node that owns
the allocation.

## The causes

| Cause | Meaning |
| --- | --- |
| `stop` | A user stopped the job, or a keyed instance of it. |
| `update` | Replaced by a newer job config, through a rollout or an in-place update. |
| `drain` | The node is draining and its work is moving elsewhere. |
| `reschedule` | Moved by the scheduler: failure recovery, `onLost` handling, or a scheduler-driven move. |
| `destroy` | The job was destroyed. |
| `node_lost` | The node was removed or declared lost, clearing its allocations. |
| `scale_in` | Surplus after `legion job scale` lowered the task's count ([autoscaling.md](autoscaling.md)). Unlike `update`, no new job config version is involved. |
| `preempted` | Displaced to make room for higher-priority work ([preemption-and-eviction.md](preemption-and-eviction.md)). Includes a `preemptor` in the stop notice. |
| `node_pressure` | The node ran short of memory and its memory pressure response stopped this task, because it was bursting above its reservation (`evict-bursting`) or the node oversubscribes memory (`evict-reserved`). The worker writes this cause into the stop notice itself. See [resource-enforcement.md](resource-enforcement.md#memory-pressure). |
| `unknown` | No cause was recorded. See [Delivery guarantees](#delivery-guarantees). |

`unknown` is a normal value, not just an error case. It most often comes from
the worker failure-report path. A task that died on its own was never stopped
by the platform, so there is no platform stop to explain.

## Where you can see it

**In the event journal.** Two event kinds cover a stop. `allocation.stopping`
(info) says the processes have entered their grace window and gives its
length. It is emitted for **every** cause, because every platform stop uses
the same ladder:

```console
$ legion events list --kind allocation.stopping -j | jq '.data.events[0]'
{
  "kind": "allocation.stopping",
  "level": "info",
  "jobId": "batch-transcode",
  "message": "Stopping 3 allocation(s) for job batch-transcode (drain), up to 30s grace",
  "metadata": {
    "count": 3, "cause": "drain", "maxGraceSeconds": 30,
    "allocations": [{"allocationId": 412, "nodeId": "node-d", "taskConfigId": 7, "graceSeconds": 30}]
  }
}
```

`allocation.removed` says the rows are gone and includes the cause in its
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

When no cause was recorded, the `cause` key is left out instead of set to
`"unknown"`, so a filter on `cause` never matches noise.

The event journal is bounded (roughly 10k events), so treat it as an
observability tool, not an audit log. For long-term records, consume
[the event stream](event-stream.md) into your own store.

**In worker logs.** The worker logs one line per stop at `info`, with the
cause:

```
INFO Stopping allocation allocation_id=412 task_name=web cause=drain
```

## How it works

1. **Recording.** Every path in the manager that removes an allocation
   writes a row to `allocation_stop_notice` **in the same transaction** as
   the `DELETE`. So a notice can never describe a stop that was rolled back,
   or one that has not been decided yet.
2. **Delivery.** Each heartbeat reply carries `stop_notices`: every live
   notice for that node, as a full snapshot instead of a delta. The worker
   replaces its copy each time.
3. **Consumption.** When the worker stops an allocation, it takes that
   allocation's notice, writes the notice file to disk, and passes the cause,
   signal, and deadline to the task runtime as a `StopContext`. Each cause
   describes one stop.
4. **Draining.** The runtime sends the signal and returns right away. The
   allocation moves into the worker's terminating set, and a reaper on the
   supervisor loop (once a second) finishes it when the process exits or the
   deadline passes. The stop never blocks the heartbeat path. A `stop()`
   that waited for a 30-second deadline would hold the task-manager mutex
   that heartbeats, the self-fence watchdog, and health reporting all need.
   At a 2-second heartbeat cadence, the manager would then declare the node
   lost and reschedule everything on it.
5. **Ordering.** Removals are applied before starts. While anything on a
   node is still in its grace window, no new allocation starts there. This
   makes plan-carried eviction physically safe. The manager's plan is
   logically atomic, and the worker handles the physical handover in order,
   so a node never runs victim and preemptor past the grace window. A new
   allocation that lands next to slow-stopping allocations waits for their
   grace period before it starts, up to the 5-minute `killAfter` cap.
6. **Worker restarts.** Stopping allocations are persisted in the worker's
   state file with where they are on the ladder. A worker that restarts
   mid-stop finishes the job. An allocation inside its grace window keeps
   its original deadline (stored as wall-clock time) and is hard-killed when
   it passes. An allocation that was still draining is signalled right
   away, because the restart already used up the time the drain was meant
   to buy. Either way, its volumes stay recorded as in use, and new starts
   on the node stay held, until its process is gone. The restarted worker
   recognizes the task by pid and process start time, the same check it
   uses for running tasks.
7. **Expiry.** Notices older than 10 minutes are deleted. The delete runs in
   the next removal's transaction instead of on the heartbeat path. Reading
   notices never writes, and a cluster with no removals does not build up
   rows to prune.

### Delivery guarantees

Stop-cause delivery is **best effort and advisory**:

- **A stop is never delayed or skipped because a cause is missing.** If the
  notice has not arrived, the worker stops the allocation with `unknown`.
- **A worker that reconciles before the notice reaches it** records
  `unknown`. In practice the notice is written before the allocation leaves
  the desired set, so it is available on the same heartbeat. This is not
  guaranteed, and nothing depends on it.
- **A worker talking to a manager older than this feature** receives no
  notices and records `unknown` for every stop. Nothing else changes.
- **Notices expire after 10 minutes.** A worker partitioned for longer than
  that reconnects and stops its allocations with `unknown`.
- **Unrecognized causes become `unknown`.** A newer manager may send a cause
  an older worker does not know. The worker stops the allocation anyway.
  This lets new causes be added without a coordinated upgrade.

The **ladder** itself is not advisory. A task always gets its signal and its
window, because the ladder runs on the worker, uses the task's own config,
and needs nothing from the manager. Only the cause in the notice file can be
lost. A stop whose notice has not arrived is still graceful. It just reports
`unknown` and has no preemptor link.

## Interaction with adjacent features

- **[Node drain](architecture-managers-workers.md):** drained allocations
  carry `drain`. Stateful allocations held back by the `onLost` gate are not
  removed at all, so they produce no notice.
- **[Rollouts](rollouts.md):** allocations replaced by a rollout carry
  `update`, which separates a planned replacement from a failure.
- **[Stateful workloads](stateful-workloads.md):** stopping a keyed instance
  carries `stop`. Destroying one carries `destroy`.
- **[Raft/replicated managers](multi-raft.md):** the scheduler lane owns
  `allocation_stop_notice`, the same lane as every removal that writes it.
  Notices replicate with the removals they describe.

## Known limitations

- **`sandbox` tasks get no grace period.** See
  [Runtime support](#runtime-support). This follows from the design (there
  is no in-guest agent).
- **The Apple `container` runtime runs its own ladder.** `stop.signal` is
  passed through, but the runtime's own timeout may fire before Legion's
  `killAfter`. Linux/OCI workers follow `killAfter` exactly.
- **The grace window adds stop latency for everyone.** Stops that used to
  be instant now take up to `killAfter`. This applies to jobs with no `stop`
  block too, because the default ladder is `SIGTERM` plus `30s` instead of
  a hard kill. Set `stop = { killAfter = "0s" }` on a task that needs the
  old behavior.
- **Starts wait behind stops on the same node.** A new allocation landing on
  a node with allocations in their grace window waits for them, even when it
  is unrelated to what is stopping. The `killAfter` cap bounds the wait. In
  return, you can rely on one guarantee: a node never runs victim and
  preemptor past the grace window.
- **The deadline in the `allocation.stopping` event is the manager's
  estimate.** The worker's actual window opens when it sends the signal, up
  to one heartbeat later.
- **No CLI filter by cause.** `legion events list --kind allocation.removed`
  returns all removals. Filter on the `cause` metadata key on the client
  side.
- **The journal records causes per removal batch, and the wire carries them
  per allocation.** A `job stop` that removes 12 allocations produces one
  `allocation.removed` event with `count = 12`, and 12 individual notices.
- **Allocation IDs are reused.** A new allocation can get an ID freed by a
  deletion. A stale notice is overwritten when the new allocation is itself
  removed. A wrong label would require a worker to stop an allocation the
  manager did not remove, which does not happen. The 10-minute expiry bounds
  the window either way.

## See also

- [Preemption and eviction](preemption-and-eviction.md): priority bands and
  the one cause that carries a preemptor link.
- [Event stream](event-stream.md): consuming `allocation.removed` and the
  rest of the journal.
- [Architecture: managers and workers](architecture-managers-workers.md):
  the heartbeat loop that carries notices.
- [Task runtime plugins](task-runtime-plugins.md): where `StopContext`
  reaches a runtime, and the plugin ABI version that guards it.
