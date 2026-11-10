# Resize

Resize changes the `memory`, `memoryMax`, or `cpu` of a unit that is already
running, in place: no new job version, no rollout, and no restart. It works
on a top-level allocation, a keyed or slot instance, a session, and a child
session.

```console
$ legion instance resize user-42 --memory 2GiB --memory-max 4GiB -j
{"ok":true,"data":{"instance":"user-42","allocation":88213,"node":"w-17",
 "from":{"memory":1024,"memoryMax":2048,"cpu":1000},
 "to":{"memory":2048,"memoryMax":4096,"cpu":1000},
 "spec":{"memory":1024,"memoryMax":2048,"cpu":1000},
 "outcome":"applied","deduplicated":false}}

$ legion alloc resize 88213 --cpu 2000 --reason "batch week"
alloc 88213: cpu 1000 -> 2000 (override active; spec cpu 1000)
```

A resize is an **override over the spec**, the same idea as `legion job
scale` for `count` ([Autoscaling](autoscaling.md)): the job's stored
`resources` do not change, and the unit carries what was set in their place.

## Why it exists

Without resize a unit's size is fixed when it starts:

- A session that turns out to need 4 GiB instead of 1 has to be stopped and
  started again, losing its state, or has to have reserved 4 GiB all along.
- `memoryMax` lets a task use memory above its reservation, but that memory
  is the first reclaimed under node pressure
  ([Resource Enforcement](resource-enforcement.md#burstable-memory)). A unit
  that needs the memory for an hour could not ask for it to be reserved.
- Editing `resources` is a deploy: a new job version and a rollout that
  replaces every allocation.

## The three steps, cheapest first

| Step | What changes | Cost |
| --- | --- | --- |
| Use up to the ceiling | Nothing. The unit uses memory between `memory` and `memoryMax`. | None |
| Resize in place | The unit's reservation, ceiling, or CPU. | None for a session; one replicated write for a top-level allocation |
| Replace | A new allocation at the new size (a deploy, or a restart). | A restart |

Resize is the second step. It never performs the third on its own: a
runtime that cannot make a change while the unit runs answers
`needs_restart`, and nothing is restarted.

## What a resize does

### A top-level allocation or keyed instance

`legion alloc resize <ALLOCATION>` and `legion instance resize <INSTANCE>`
(for a keyed or slot instance, which resizes its current allocation) do the
same thing:

1. The manager asks the allocation's node whether its runtime can make the
   change. `needs_restart` and `not_supported` are answered here, before
   anything is committed.
2. One compare-and-set on the allocation's row commits the new size: `from`
   is the effective size the manager read, `to` the new one. A second
   writer that lost the race is answered `FAILED_PRECONDITION` with `stale`
   and reads again.
3. The node applies the change through the task runtime.

The order of steps 2 and 3 depends on the direction, so a unit is never
running above what is reserved for it:

- **A grow reserves, then applies.** A grow is a larger `memory` or `cpu`.
  It takes only free room on the allocation's node and never preempts other
  work. If the node then cannot apply it, the reservation is given back.
- **A shrink applies, then releases.** The node lowers the unit's limits
  first, and the room goes back to the scheduler only after that.

A change to `memoryMax` alone is never a grow: the ceiling is enforced on
the node and is not something the scheduler reserves.

The row's resources are the unit's **effective** resources from then on.
Placement, `legion node usage`, the ledger, preemption, and the capacity
recheck in plan apply all read the row, so they all see the new size.

### A session

A session's resize is its node's alone. The manager routes `legion instance
resize <SESSION>` to the node the session id's tag names. The node admits a
grow against the session task's [envelope](session-mode.md) (the envelope's
free room is the only room a session can take) and tells the runtime.
Nothing is written to the replicated store, whatever the outcome.

- A grow the envelope has no room for is `refused`, with the envelope's
  free room.
- When the envelope is [elastic](session-mode.md#elastic-envelopes) and its
  `max` can hold the grow, the answer is `deferred` and the grow counts
  toward the envelope's size: the node asks its manager for the room on its
  next heartbeat, as it does when a session start fills the envelope, and a
  retry a few seconds later is applied. A deferred grow that is not retried
  within 60 seconds stops counting.
- A session's size is part of the session. It survives a worker restart and
  a suspend and resume: the session resumes at the size it was resized to.

A suspended session cannot be resized; start it first.

### A child session, from inside its owner

An owner can resize its own [child sessions](child-sessions.md) through its
node's API socket, with no token:

```console
$ legion instance resize machine-q4n7c2rd.01jbz... --memory 4GiB --local
```

Only the caller's own children are reachable, and only a child on the
owner's node. A child on another node is resized through a manager.

## What each runtime can change

| Runtime | `memory` (reservation) | `memoryMax` (ceiling) | `cpu` |
| --- | --- | --- | --- |
| `exec`, `container` | Live: the cgroup's `memory.low`. | Live: the cgroup's `memory.max`. A ceiling below what the unit uses now is `refused` unless `--force`. | Live: the cgroup's `cpu.weight`. |
| `sandbox` | Live: `memory.low` on the VMM's cgroup, and the floor the pressure response reclaims down to. | Down: the balloon inflates by the difference. Back up to the boot size: the balloon deflates. Above the boot size: `needs_restart`. | Live for the VMM's threads (`cpu.weight`). The guest keeps its vCPU count. |
| `envelope` | Not resized this way. A session envelope resizes through `stateful.envelope` ([elastic envelopes](session-mode.md#elastic-envelopes)). | | |
| Every other runtime, including plugins that predate resize | `not_supported` | `not_supported` | `not_supported` |

### Sandboxes

A microVM's memory is fixed when it boots, at `memoryMax` (or `memory`
when there is no `memoryMax`). A resize moves the guest's usable memory
below that size and back up to it with the virtio-balloon, on both
Firecracker and cloud-hypervisor:

```console
$ legion instance resize ws-7 --memory-max 16GiB -j
{"ok":false,"error":{"code":"needs_restart",
 "message":"sandbox ws-7 booted with a 4096 MiB guest; firecracker cannot grow it past that while it runs",
 "hint":"restart the instance to start it at the new size (a redeploy with the new resources does the same for every allocation of the task)",
 "retryable":false,
 "details":{"outcome":"needs_restart","instance":"ws-7","node":"w-17", ...}}}
```

- A guest that booted **without** a balloon cannot lower its ceiling while
  it runs, and answers `needs_restart`. A guest boots with a balloon when
  its `memoryMax` is above its `memory`, or when its task takes memory
  checkpoints (a `suspend` or `snapshots` block).
- The cgroup's `memory.max` stays at the boot size plus the VMM overhead.
  The balloon is what limits the guest; a cgroup ceiling under a guest that
  has not handed its pages back yet would kill the VMM.
- The [memory pressure response](resource-enforcement.md#memory-pressure)
  still reclaims from a resized guest. When the episode ends, the balloon
  returns to the size the resize holds it at.
- With a dev backend (a container standing in for the VM) the sandbox
  resizes like a container.

### When resources are not enforced

On a worker with `resourceEnforcement` off, on macOS, and for a task that
declared no resources, a unit's resources are scheduling-only: nothing
confines it. A resize is then an accounting change. The reservation moves,
the reply is `applied`, and no limit is written, exactly as none was written
at start.

## Outcomes

Every resize reply carries an `outcome`. Only `applied` changed anything.

| Outcome | Meaning | CLI error code | Retry |
| --- | --- | --- | --- |
| `applied` | The unit runs at the new size. | (success) | |
| `deferred` | The session's elastic envelope is growing to make room. | `deferred` | Yes, in a few seconds |
| `refused` | A grow does not fit, or a ceiling would go below current use. | `resource_exhausted` for a grow that found no room, else `refused` | A grow: yes, once room frees up |
| `needs_restart` | The runtime cannot make this change while the unit runs. | `needs_restart` | No |
| `not_supported` | The runtime, or the node's worker version, cannot resize. | `not_supported` | No |

The CLI exits non-zero for every outcome but `applied`, with the standard
error envelope. A refused grow says what the node has:

```console
$ legion alloc resize 88213 --memory 64GiB -j
{"ok":false,"error":{"code":"resource_exhausted",
 "message":"node w-03 has 11GiB free; the grow needs 63GiB more",
 "hint":"resize to at most 12GiB here, or move the allocation",
 "retryable":true,
 "details":{"outcome":"refused","alloc":"88213","node":"w-03",
  "from":{"memory":1024,"cpu":1000},"to":{"memory":65536,"cpu":1000},
  "free":{"memory":11264,"cpu":3000}}}}
```

`free` is the room beyond what the unit already holds: the node's for an
allocation, the envelope's for a session.

Requests that cannot be acted on at all are gRPC errors:

| Code | When |
| --- | --- |
| `INVALID_ARGUMENT` | No amount given, a non-positive amount, or a `memoryMax` below `memory` |
| `NOT_FOUND` | No such allocation, instance, or session |
| `FAILED_PRECONDITION` | The allocation is a session envelope, the instance is suspended or stopped, or the row changed under the call (`stale`) |
| `UNAVAILABLE` | The unit's node could not be reached. Nothing stays changed: a shrink was never committed, and a grow's reservation is given back |

## The surface

```console
$ legion alloc resize <ALLOCATION> [--memory SIZE] [--memory-max SIZE] [--cpu MHZ]
      [--reason TEXT] [--force] [--dry-run] [--idempotency-key KEY] [-j]
$ legion instance resize <INSTANCE|SESSION> [same flags] [--local]
```

- **Sizes.** `--memory` and `--memory-max` take `512`, `512MiB`, or `2GiB`
  (a bare number is MiB). `--cpu` is MHz. An amount you leave out keeps its
  current value. `memoryMax` can never be below `memory`; raise both in one
  call when the reservation would pass the ceiling.
- **`--dry-run`** answers what a real call would, without changing anything:
  whether the runtime can make the change and whether it fits. A dry run's
  answer is data, so the CLI exits zero and prints the `outcome`, and for
  anything but `applied` the `message`, `hint`, and `free`.
- **`--force`** allows a ceiling below what the unit uses now. The kernel
  reclaims what it can and may kill the task.
- **`--reason`** is recorded on the override and on the event.
- **`--idempotency-key`** makes a retry of an allocation or keyed-instance
  resize return the first reply with `deduplicated: true`. A session's
  resize records no key: it sets absolute amounts on one node, so repeating
  it is safe and answers `applied`.
- **RPCs.** `Manager.ResizeAllocation` and `Manager.ResizeInstance`, and
  `Worker.ResizeSession`, which only a manager calls. From inside a task,
  `Workload.ResizeChildSession` on the API socket.
- **Agents.** MCP tools `legion_instance_resize` and `legion_alloc_resize`
  (mutating, behind `--allow-write`), and `instance.resize` and
  `alloc.resize` in `legion capabilities`. See
  [Agent Readiness](agent-readiness.md).

### Permissions

Resize needs `scale:write` on the unit's job, the same permission `legion
job scale` needs, so a principal can resize without redeploy rights
([Auth and ACLs](auth-and-acls.md)). The caller must hold `scale:write` on
some job before the unit is looked up, so an id's existence cannot be
probed without it.

### Seeing the override

- `legion instance get <ID>` prints the effective resources, and for a
  resized keyed instance the override: what the spec says, the reason, who
  resized, and when. In JSON these are `resources` and `resourcesOverride`.
- `GetLedger` reports each allocation's effective `resources` and, when it
  was resized, `resources_override`. `legion ledger` prints the effective
  `resources`.
- The UI's instance and allocation pages show the effective reservation and
  a "Resized in place" line with the same details.
- Events `allocation.resized` and `instance.resized` carry `from`, `to`,
  `spec`, `reason`, and `by` ([Event Stream](event-stream.md)).
- `legion_resizes_total{unit, outcome}` counts answered resizes
  ([Metrics](metrics.md)).

A session's resize emits no event, because an event is a replicated write
and a session's resize makes none. It is in the node's log, the metric, and
the session's own `resources`.

## How long an override lasts

- **It ends with the unit.** An allocation's override is on its row. A
  replacement allocation (a reschedule after node loss, a rollout, a keyed
  instance that was suspended and resumed, a restart that places a new
  allocation) starts at the spec. A session's override ends with the
  session.
- **A resize back to the spec's amounts removes it.** There is nothing
  else to reset.
- **A deploy that changes `resources` replaces the allocations**, as every
  spec change does, so the new spec wins.
- An allocation restarted in place by its restart policy keeps its override:
  the node restarts it at its effective size.

## Failure modes

- **The node is unreachable.** `UNAVAILABLE`. A shrink was never committed.
  A grow's reservation is given back; if that write also fails, the node
  converges the allocation to the committed size on its next heartbeat.
- **The call to the node is lost after a grow was committed.** The desired
  set the node receives on every heartbeat carries the allocation's
  effective resources. A node that runs an allocation at a different size
  resizes it in place, once per distinct size, and logs a warning when its
  runtime cannot.
- **A manager loses leadership mid-resize.** The compare-and-set is
  forwarded to the scheduler lane's leader like every scheduler write. A
  retry with the same idempotency key is deduplicated; without one, the
  compare-and-set makes the retry safe.
- **A worker restarts.** A resized allocation's limits are still in its
  cgroup, and the worker's persisted record holds the new size. A resized
  session is re-adopted at its new size.
- **A worker that predates resize.** The manager never sends it one and
  answers `not_supported`, naming the node and its worker version.

## Known limitations

- **No hotplug.** A sandbox cannot grow past the memory it booted with,
  on either VMM, and its vCPU count never changes while it runs.
- **`cores` and devices do not resize.** A task that reserves whole cores
  (`resources.cores`) keeps them; GPUs and other devices are fixed at
  placement.
- **A grow never preempts.** It takes free room only. When the node is
  full, the answer is `refused` with the room there is.
- **Per unit only.** There is no command that resizes every allocation of a
  task at once, and none that moves a unit to another node to resize it;
  restart or redeploy for those.
- **An override does not follow a replacement.** See "How long an override
  lasts".
- **A task that declared no resources is not confined by a resize.** The
  reservation moves, but a unit that started without limits keeps running
  without them until it restarts.
- **Promoting a resized session returns it to its spec's reservation.**
  The promoted instance's allocation is written at the task's spec. Resize
  the instance after promoting it to reserve the larger size.
- **A child on another node** cannot be resized from inside its owner; use
  `legion instance resize` through a manager.
- **A shrink raced by a second writer** is put back: if the row changed
  between the node's apply and the manager's release, the node is asked to
  return the unit to its previous size and the call answers `stale`.
- **The sandbox path is covered by unit tests of its decisions and by
  KVM tests that do not run in CI.** The balloon calls are exercised against
  a fake VMM socket in the ordinary suite.
