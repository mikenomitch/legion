# Resize

Resize changes the `memory`, `memoryMax`, or `cpu` of a unit that is already
running, in place: no new job version, no rollout, and no restart. It works
on a top-level allocation, a keyed or slot instance, a session, a child
session, and, with `legion job resize`, on every allocation of a task at
once. A unit that cannot grow where it is can be resized by moving it
(`--move`).

```console
$ legion instance resize user-42 --memory 2GiB --memory-max 4GiB -j
{"ok":true,"data":{"instance":"user-42","allocation":88213,"node":"w-17",
 "from":{"memory":1024,"memoryMax":2048,"cpu":1000},
 "to":{"memory":2048,"memoryMax":4096,"cpu":1000},
 "spec":{"memory":1024,"memoryMax":2048,"cpu":1000},
 "outcome":"applied","deduplicated":false}}

$ legion alloc resize 88213 --cpu 2000 --reason "batch week"
alloc 88213: cpu 1000 -> 2000 (override active; spec cpu 1000)

$ legion job resize web --memory 1GiB
shop/web: memory 512MiB -> 1GiB (override active; spec memory 512)
  allocations: 3 resized in place, 0 kept their size, 0 replaced
```

```console
$ legion instance resize ws-7 --memory 16GiB --move
instance ws-7: memory 4GiB -> 16GiB (override active; spec memory 4096) [moved to w-22]
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
| Move | The unit is suspended and resumed at the new size on a node with room (`--move`). | One suspend and resume |
| Replace | A new allocation at the new size (`legion job resize --replace`, a deploy, or a restart). | A restart |

Resize is the second step. It never performs the others on its own: a
runtime that cannot make a change while the unit runs answers
`needs_restart`, a grow that does not fit answers `refused`, and nothing is
moved or restarted unless the caller passed `--move` or `--replace`.

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

## Resizing a task

`legion job resize` sets a **task-level override**: one record on the task,
beside the count override `legion job scale` keeps, laid over the task's
`resources`.

```console
$ legion job resize web --task api --memory 1GiB --cpu 2000 -j
{"ok":true,"data":{"job":"web","group":"default","task":"api",
 "from":{"memory":512,"cpu":1000},"to":{"memory":1024,"cpu":2000},
 "spec":{"memory":512,"cpu":1000},"overrideActive":true,
 "applied":2,"pending":1,"replaced":0,
 "allocations":[
  {"allocation":88213,"node":"w-03","outcome":"applied","from":{"memory":512,"cpu":1000},"to":{"memory":1024,"cpu":2000}},
  {"allocation":88214,"node":"w-04","outcome":"applied","from":{"memory":512,"cpu":1000},"to":{"memory":1024,"cpu":2000}},
  {"allocation":88215,"node":"w-09","outcome":"refused","from":{"memory":512,"cpu":1000},"to":{"memory":1024,"cpu":2000},
   "message":"node w-09 has 256MiB free; the grow needs 512MiB more","free":{"memory":256,"cpu":4000}}],
 "hint":"1 allocation(s) kept their old size and take the new one when they are next replaced; pass --replace to stop them now so the scheduler places their replacements at it",
 "deduplicated":false}}
```

While the override stands:

- **Every running allocation of the task is brought to it in place**, one
  at a time, exactly as `legion alloc resize` would, and the reply carries
  one answer per allocation.
- **An allocation that cannot change in place keeps its old size** (no room
  on its node, a runtime that answers `needs_restart` or `not_supported`, a
  node that could not be reached) and takes the new size when it is next
  replaced. `pending` counts these. Nothing is restarted.
- **`--replace` stops those allocations now**, so the scheduler places
  their replacements at the new size. They are stopped together, in one
  write, with the stop cause `reschedule`; use it when the task can take
  that many replicas restarting at once.
- **Every new allocation of the task starts at the overridden size**: a
  scale-out, a reschedule after a node is lost, a restart that places a new
  allocation, and a rollout of a new version whose `resources` are
  unchanged. The scheduler packs the overridden size, and the allocation's
  row records the override.
- **`--reset` removes the override** and brings the running allocations
  back to the spec's size, in place where they can be.

No job version is created and no rollout starts. `legion job scale <JOB>
--status -j` (`GetJobScaleStatus`) reports the task's override in
`resourcesOverride`, with its reason, who set it, and when.

**The spec wins when it changes.** A submit that changes the task's
`memory`, `memoryMax`, or `cpu` drops the override and emits `resize.reset`;
a submit that leaves them alone keeps it. This is the rule count overrides
follow ([Autoscaling](autoscaling.md)). A task the new version no longer
has loses its override too.

Session tasks are refused: a session task's room is its envelope
(`stateful.envelope`), and each session is resized on its own.

## When it does not fit: move

A grow that finds no room on the unit's node is `refused`, and a sandbox
asked to grow past the memory it booted with answers `needs_restart`. For
a unit that can move, `--move` resizes it by moving it:

```console
$ legion instance resize ws-7 --memory 16GiB --move -j
{"ok":true,"data":{"instance":"ws-7","allocation":88301,"node":"w-22",
 "from":{"memory":4096,"cpu":2000},"to":{"memory":16384,"cpu":2000},
 "spec":{"memory":4096,"cpu":2000},"outcome":"applied","movedTo":"w-22",
 "deduplicated":false}}
```

1. The unit's new size is recorded with it.
2. It is suspended, through the path `legion instance suspend` uses.
3. It is resumed, through the path `legion instance start` uses, and placed
   at the new size on a node with room.

**A sandbox's guest size changes only at boot.** A memory checkpoint
restores only into a guest of the size it was taken at
([Sandbox Runtime](task-runtime-sandbox.md#where-a-memory-checkpoint-can-be-restored)),
and a guest's size is its `memoryMax` (or its `memory` when it has none). So:

- A keyed sandbox moved to a **different guest size** is suspended to a
  **disk** checkpoint and boots fresh at the new size: its disk is kept and
  its memory is not. The reply's `message` and a `--dry-run` both say so.
  This is the restart `needs_restart` asks for, done for you and placed
  where it fits.
- A keyed sandbox moved with its guest size unchanged (a larger
  reservation under the same `memoryMax`, or more `cpu`) keeps its memory.
- A sandbox **session** is refused a move that would change its guest size
  (`FAILED_PRECONDITION`): a session resumes only from its memory
  checkpoint. It can move for a larger reservation or more `cpu`.

| Unit | Can move when | Where it resumes |
| --- | --- | --- |
| Keyed instance | its task has a `suspend` block | wherever the scheduler places its resume; a checkpoint that lives only on its node (`suspend.durability = "local"`) keeps it on that node, so the move helps a `needs_restart` but not a full node |
| Session | its task sets `suspend.durability = "store"`, and for a sandbox the guest size does not change | another node whose envelope for the task has room, from its manifest in the content store |
| Anything else | never | refused with `FAILED_PRECONDITION`, naming what is missing |

A move interrupts the unit for one suspend and resume, so it needs
`job:write` as well as `scale:write`.

**A move that finds no room puts the unit back.** A session that no other
node takes is resumed on the node it came from at the size it had. A keyed
instance that is not placed at the new size within 60 seconds has its size
record restored and is placed at the size it had. Either way the outcome is
`refused` and retryable. `--dry-run` says whether the unit can move and
what its size would become; whether a node has the room is only known when
it resumes.

## What each runtime can change

| Runtime | `memory` (reservation) | `memoryMax` (ceiling) | `cpu` |
| --- | --- | --- | --- |
| `exec`, `container` | Live: the cgroup's `memory.low`. | Live: the cgroup's `memory.max`. A ceiling below what the unit uses now is `refused` unless `--force`. | Live: the cgroup's `cpu.weight`. |
| `sandbox` | Live: `memory.low` on the VMM's cgroup, and the floor the pressure response reclaims down to. | Down: the balloon inflates by the difference. Back up to the boot size: the balloon deflates. Above the boot size: `needs_restart`. | Live for the VMM's threads (`cpu.weight`). The guest keeps its vCPU count. |
| `isolate` (session isolates) | Live: the reservation the isolate is charged against its envelope; refused when it does not fit the envelope. | Live, up and down, but never below the memory the isolate uses now (`refused`, a resize never takes memory back); above the worker's `isolate.maxMemory` (one memory slot), `needs_restart`. Kept across hibernation. | `not_supported`: an isolate's CPU is its task's weight in the host's scheduler, not a per-session amount. |
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
 "hint":"resize to at most 12GiB here, or `legion job resize --replace` to place the task's allocations at the new size elsewhere",
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
| `FAILED_PRECONDITION` | The allocation is a session envelope, the instance is suspended or stopped, the row changed under the call (`stale`), `--move` on a unit that cannot move, or `job resize` on a session task |
| `UNAVAILABLE` | The unit's node could not be reached. Nothing stays changed: a shrink was never committed, and a grow's reservation is given back |

## The surface

```console
$ legion alloc resize <ALLOCATION> [--memory SIZE] [--memory-max SIZE] [--cpu MHZ]
      [--reason TEXT] [--force] [--dry-run] [--idempotency-key KEY] [-j]
$ legion instance resize <INSTANCE|SESSION> [same flags] [--move] [--local]
$ legion job resize <JOB> [--task TASK] [--group GROUP] [same flags] [--replace]
$ legion job resize <JOB> [--task TASK] --reset
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
- **`--idempotency-key`** makes a retry return the first reply with
  `deduplicated: true`. For an allocation, a keyed instance, and a task the
  managers record the key. For a session its node keeps the answer of an
  applied resize for 10 minutes, in memory: the key is per node, as for
  [session runs](session-runs.md), and a worker restart forgets it. A
  session's resize sets absolute amounts, so the repeat that then happens
  answers `applied` with `from` equal to `to`.
- **`--move`** (instances and sessions) and **`--replace`** and
  **`--reset`** (tasks) are described above.
- **RPCs.** `Manager.ResizeAllocation`, `Manager.ResizeInstance`
  (`move_unit` for `--move`), and `Manager.ResizeTask`; and
  `Worker.ResizeSession`, which only a manager calls. From inside a task,
  `Workload.ResizeChildSession` on the API socket.
- **Agents.** MCP tools `legion_instance_resize` (with `move`),
  `legion_alloc_resize`, and `legion_job_resize` (mutating, behind
  `--allow-write`), and `instance.resize`, `alloc.resize`, and `job.resize`
  in `legion capabilities`. See [Agent Readiness](agent-readiness.md).

### Permissions

Resize needs `scale:write` on the unit's job, the same permission `legion
job scale` needs, so a principal can resize without redeploy rights
([Auth and ACLs](auth-and-acls.md)). The caller must hold `scale:write` on
some job before the unit is looked up, so an id's existence cannot be
probed without it. `legion job resize` needs it on the job it names.
`--move` also needs `job:write`, because it suspends and resumes the unit.

### Seeing the override

- `legion instance get <ID>` prints the effective resources, and for a
  resized keyed instance the override: what the spec says, the reason, who
  resized, and when. In JSON these are `resources` and `resourcesOverride`.
- `GetLedger` reports each allocation's effective `resources` and, when it
  was resized, `resources_override`. `legion ledger` prints both: a
  "Resized" line under the allocation, and `resourcesOverride` in JSON.
- `legion job scale <JOB> --status -j` reports a task-level override.
- A suspended or stopped instance that carries a size shows it in
  `resourcesOverride`: the size its next allocation starts at.
- The UI's instance and allocation pages show the effective reservation and
  a "Resized in place" line with the same details.
- Events `allocation.resized` and `instance.resized` carry `from`, `to`,
  `spec`, `reason`, and `by`; a move adds `movedFrom` and `movedTo`.
  `task.resized` carries the task's `from`, `to`, and how many allocations
  were applied, kept, and replaced; `resize.reset` says a submit dropped a
  task's override ([Event Stream](event-stream.md)).
- `legion_resizes_total{unit, outcome}` counts answered resizes
  ([Metrics](metrics.md)).

A session's resize emits no event, by design: an event is a replicated
write, and a session's resize makes none. It is in the node's log, the
metric, and the session's own `resources`. The same holds for a session's
resize by moving.

## How long an override lasts

- **A plain allocation's override ends with the allocation.** `legion
  alloc resize` on a service or batch allocation is on its row; its
  replacement starts at the task's size (the spec, or the task-level
  override when there is one).
- **An instance's size stays with the instance.** A resize of a keyed or
  slot instance is recorded on the instance as well as on its allocation,
  so its next allocation (a resume after a suspend, a restart, a reschedule
  after its node is lost, a move) starts at the same size. If the node
  chosen has no room for it, that node is skipped and another candidate is
  tried.
- **A session's size stays with the session.** It survives a suspend and
  resume on its node, and for a `store` session a resume on another node:
  the size travels in the session's manifest. It ends with the session.
- **A task-level override stays until it is reset or the spec changes.**
- **Order of precedence** for a new allocation: the instance's own size,
  then the task-level override, then the spec. `legion job resize` clears
  the own size of every instance it resizes in place, so the task's size
  governs them from then on.
- **A resize back to the spec's amounts removes the override.**
- **A submit that changes the task's `resources` wins.** It drops the
  task-level override, and instance sizes recorded over the old spec no
  longer apply: every override binds only to the spec it was laid over.
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
  on either VMM, and its vCPU count never changes while it runs. A restart
  boots it at its new size.
- **A sandbox resized and then suspended resumes cold.** Its memory
  checkpoint holds the guest at the size it booted with, and the resume
  boots at the resized size, so the shapes differ and the restore is
  refused; `onRestoreFailure` decides (a cold boot by default).
- **`cores` and devices do not resize.** A task that reserves whole cores
  (`resources.cores`) keeps them; GPUs and other devices are fixed at
  placement.
- **A grow never preempts.** It takes free room only. When the node is
  full, the answer is `refused` with the room there is.
- **`--replace` is not a rollout.** It stops every allocation that could
  not change in place in one write; it has no batch size, health gate, or
  pause. Use a deploy when replacements must be paced.
- **A task resize is not atomic across allocations.** Each allocation is
  its own compare-and-set, so a call interrupted midway leaves some resized
  and the override in place; repeat it (it is idempotent) to finish.
- **A keyed instance moves only as far as its checkpoint can.** With a
  checkpoint kept on its node, `--move` resumes it on the same node.
- **The scheduler packs a keyed task at its largest waiting instance.**
  While a resized instance waits for an allocation, every allocation of its
  task planned in that pass is packed at that size, which can pass over a
  node a smaller instance would have fit. The row is still written at each
  instance's own size.
- **A move waits.** `--move` on a keyed instance holds the call until the
  suspend finishes (up to 3 minutes) and the resume is placed (up to 60
  seconds). A suspend that outlasts the wait leaves the new size recorded:
  `legion instance start` resumes the instance at it.
- **A plain allocation's own override does not follow its replacement.**
  See "How long an override lasts".
- **The UI has no task-level line.** It shows the override on each
  allocation and instance a task resize sized (the same "Resized" line a
  unit resize gets); the task's own record is in `legion job scale
  --status -j`.
- **The move paths are tested without a real runtime.** The keyed move runs
  in the ordinary suite against reported checkpoints; a `store` session's
  move needs a content store and a runtime that checkpoints memory, and is
  covered only by the unit tests of its parts.
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
