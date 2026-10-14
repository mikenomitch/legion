# Suspend and Resume

Agent workspaces, dev boxes, and long-running sandboxes spend most of their
life waiting on a human. While they wait they hold their full memory
reservation, and stopping them throws away the warm language server, the
loaded model, and the half-finished build. Suspend is the third option: Legion
checkpoints the instance, stops it, and frees its reservation, and a later
`start` brings it back where it left off.

This page covers **suspend and resume** first, then **snapshots**, the
building block underneath, then **fork** and **creating instances from a
snapshot**. A snapshot is a point-in-time capture of a running instance. A
suspend is a snapshot the instance owns, taken on the way to stopping it. A
fork is a snapshot that new instances restore from while the original keeps
running.

To run a fleet of agent sandboxes on these pieces, start with the
[agent sandboxes guide](guides/agent-sandboxes.md).

> Related: [stateful workloads](stateful-workloads.md) (instances and their
> identity), [volumes](volumes.md#snapshot-restore-and-clone) (the volume
> `snapshot` op), [the sandbox runtime](task-runtime-sandbox.md#checkpoints)
> (memory checkpoints of microVMs),
> [workload identity](workload-identity.md#suspended-and-restored-instances),
> [the node content store](artifacts.md#the-node-content-store) (where
> memory checkpoints live), and
> [publishing content](artifacts.md#publishing-content) (where durable
> checkpoints go).

## The short version

```nickel
{
  name = "agents",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    resources = { memory = 512, memoryMax = 1024, cpu = 1000 },
    stateful = {
      mode = "keyed",
      storage = { path = "/workspace", mode = "block", params = { size = "10G" } },
    },
    suspend = {
      checkpoint = "memory",
      onResume = ["/usr/local/bin/reconnect"],
    },
    config = {
      image = "ghcr.io/acme/workspace:latest",
      command = ["/usr/local/bin/agent-shell"],
    },
  }],
}
```

```console
$ legion instance create agents --id user-42
$ legion instance suspend user-42 --wait -j
{"ok":true,"data":{"instance":"user-42","state":"suspended",
 "checkpoint":"snap-91c2a0b3d4e5f601","kind":"memory","bytes":1918893056,
 "durationMs":740,"node":"w-03","durability":"local", ...}}

$ legion instance list agents
ID       STATE      ...  CHECKPOINT
user-42  suspended  ...  snap-91c2a0b3d4e5f601 (memory, local w-03)

$ legion instance start user-42 --wait -j
{"ok":true,"data":{"instance":"user-42","state":"active",
 "resumedFrom":"snap-91c2a0b3d4e5f601","node":"w-03","restoreMs":212, ...}}
```

While `user-42` is suspended it holds no allocation and no reservation, which
`legion node usage w-03` shows. The resumed sandbox continues with the
processes, memory, and disk it had, under a new allocation id with a freshly
issued identity. Every command takes `-j` for the standard JSON envelope.

## The `suspended` state

`suspended` is an instance state of its own, a peer of `stopped`:

| | `stopped` | `suspended` |
| --- | --- | --- |
| Who puts it there | `instance stop`, or a job stop | `instance suspend`, the [idle policy](#idle-policy), or the node's [`suspend` pressure step](#under-memory-pressure) |
| Allocation | Deleted | Deleted (kept only with `suspend.reserve`) |
| Resources reserved | None | None, unless `suspend.reserve = true` |
| Storage | Retained | Retained |
| Checkpoint | None | `checkpoint_id` names a snapshot |
| `start` does | Cold boot | Restore from the checkpoint (`--cold` boots fresh) |
| Counts toward desired | No | No |

Suspend is for keyed stateful instances (`stateful.mode = "keyed"`), and for
sessions (`stateful.mode = "session"`) whose task has a `suspend` block: a
session with `durability = "local"` suspends to its own node's disk and
resumes there under the same id; one with `durability = "store"` also
publishes its checkpoint and a session manifest to the node's
`contentStore`, and resumes on any node with an envelope for its task,
claiming a new generation so only one copy runs. Sessions add `keepFor`,
`checkpointEvery`, and `onWorkerStop`. See
[Idle sessions](session-mode.md#idle-sessions) and
[Moving a session](session-mode.md#moving-a-session). A
suspended instance survives a job stop: it holds nothing, and its checkpoint
waits for the next `start`. `instance stop` on a suspended instance discards
the checkpoint (the next `start` boots cold), and `instance destroy` discards
it with the rest of the instance.

## The `suspend` block

The block sets how a task suspends and resumes. `legion instance suspend`
works without it, with the defaults below.

```nickel
suspend = {
  checkpoint = "memory",          # "memory" | "disk"
  reserve = 0.25,                 # keep a share of the reservation while suspended
  onRestoreFailure = "cold",      # "cold" | "fail"
  resumeTimeout = "30s",          # how long a resume waits for its node
  onResume = ["/usr/local/bin/reconnect"],  # run in the guest after a restore
  durability = "local",           # "local" | "store"
  archive = "slim",               # "slim" | "manifestOnly": keep the record in the bucket (store only)
  idle = { after = "3m" },        # suspend automatically when quiet
  onPressure = true,              # let a short node suspend it instead of evicting
  wake = { port = "http" },       # resume on a request through `legion start gateway`
},
```

| Field | Default | Meaning |
| --- | --- | --- |
| `checkpoint` | `memory` for a `sandbox`, `disk` otherwise | What a suspend captures. See [Memory or disk](#memory-or-disk). |
| `reserve` | `false` | How much of the instance's reservation stays charged to its node while suspended: `false`, `true`, a ratio between 0 and 1, or fixed amounts `{ memory = "1GiB", cpu = 100 }`. See [Capacity for resumes](#capacity-for-resumes). |
| `onRestoreFailure` | `cold` | When a resume cannot restore: `cold` boots fresh on the retained storage, `fail` leaves the instance suspended with the reason in its `detail`. |
| `resumeTimeout` | `30s` | How long a resume waits to be placed on its checkpoint's node before `onRestoreFailure = "cold"` boots it fresh. At least one second. |
| `onResume` | none | argv the sandbox guest runs after every memory restore, once its identity is refreshed. |
| `durability` | `local` | Where the checkpoint lives. `local`: only on the node that took it. `store`: also published to that node's `contentStore`, so the instance can resume on any compatible node and survives losing the node. See [Durability](#durability-surviving-the-node). |
| `archive` | none | Once a `durability = "store"` checkpoint is published, move its record out of the replicated store and into the bucket: `slim` keeps a small instance row, `manifestOnly` keeps nothing for an unnamed instance. See [Archiving idle instances](#archiving-idle-instances). |
| `idle` | none | Suspend automatically once the instance has been quiet for `idle.after`. See [Idle policy](#idle-policy). |
| `onPressure` | `false` | Let the node's memory pressure response suspend this instance (the `suspend` step) before it evicts anything. See [Under memory pressure](#under-memory-pressure). |
| `keepFor`, `checkpointEvery`, `onWorkerStop` | `24h`, off, `keep` | Session tasks only: how long a suspended session's local checkpoint is kept, a periodic live checkpoint, and whether a stopping worker checkpoints running sessions. See [Idle sessions](session-mode.md#idle-sessions). With `durability = "store"` each of these checkpoints is published and written to the session's manifest, and `stateful.fenceAfter` (default `2m`) sets when a node cut off from managers and the bucket suspends its running sessions ([No split brain](session-mode.md#no-split-brain)). |
| `wake` | none | `{ port, timeout = "30s" }`: requests through `legion start gateway` for this instance go to the `network.ports` label `port`, and a request for a suspended instance resumes it and is held until it serves. Without it, the gateway refuses the instance. See [Waking on a request](#waking-on-a-request). |

Submit refuses anything the block cannot honor, with the fix in the hint:

```text
Error: task "api" sets `suspend` but is not a keyed stateful task
Hint: suspend applies to keyed instances (stateful.mode = "keyed"); for stateless services, scale with `legion job scale`

Error: task "api" cannot take a memory checkpoint: runtime "container" supports "disk" only
Hint: set suspend.checkpoint = "disk", or run this task with runtime = "sandbox"

Error: task "workspace": suspend.reserve 1.5 is out of range: a ratio is between 0 and 1 (0.25 holds a quarter of the instance's memory and cpu while it is suspended)
Hint: use true to hold everything, or a fixed amount like { memory = "1GiB", cpu = 100 }

Error: task "workspace": suspend.reserve.memory (4096 MiB) exceeds the task's resources.memory (2048 MiB). A suspended instance cannot hold more than it reserves while running
Hint: lower suspend.reserve.memory, or use reserve = true to hold the whole reservation
```

`onResume` on a task that resumes with a cold boot (any runtime but `sandbox`,
or `checkpoint = "disk"`) is refused too, and so is `durability = "store"` on
a task whose instance data lives on one node:

```text
Error: task "workspace" sets suspend.durability = "store", but its stateful.storage uses volume driver `local-dir`, which keeps the data on one node. A published checkpoint could resume on another node without its volume
Hint: use a network volume driver (such as `nfs`) for stateful.storage, drop stateful.storage, or keep durability = "local"
```

The `hint` idle signal is not available, and a spec that sets it fails with
a hint rather than being accepted and ignored.

### Memory or disk

| Checkpoint | What a suspend does | What a resume does | Runtimes |
| --- | --- | --- | --- |
| `memory` | Pauses the guest, writes its memory, device state, and root disk into the node content store, then stops the VMM | Loads the guest where it was paused, then refreshes its identity | `sandbox` on Firecracker or cloud-hypervisor |
| `disk` | Stops the task. The instance volume stays where it is, uncopied | Boots the task fresh on that volume | Any runtime |

A memory checkpoint needs the sandbox to have launched ready for checkpoints,
which a `suspend` block (or a [`snapshots` block](#the-snapshots-block)) does
at boot. Adding the block to a running job applies to allocations started
after the change, so restart an instance to make it memory-suspendable.

A disk suspend of an `exec` or `container` task is a stop that remembers it
was a suspend. Nothing from process memory survives it (Legion does not use
CRIU), but the volume does, and the resumed task finds every file it wrote.

A sandbox's writes sit in the guest's page cache until the guest writes them
back, so both kinds flush the guest first (`sync` through the guest agent,
for up to 10 seconds). A memory suspend flushes before it pauses the guest,
so the volume has every earlier write on its own, not only in the captured
memory. A disk suspend flushes before it kills the VMM, so the resumed
sandbox finds every file it wrote. A guest whose agent does not answer is
suspended without the flush, and the node logs `Could not flush the sandbox
guest's disks`. A disk suspend of such a guest loses the writes it had not
written back. See
[Stopping a sandbox](task-runtime-sandbox.md#stopping-a-sandbox).

## Suspending

```console
$ legion instance suspend <instance> [--checkpoint memory|disk] [--wait] [--timeout SECONDS]
```

1. The manager records a `purpose = suspend` snapshot in state `capturing` and
   marks the instance `suspending`. The call returns here (`--wait` keeps
   going).
2. On its next heartbeat, the instance's node captures the allocation and
   then stops it. For a memory checkpoint the guest stays paused from the
   capture until the VMM is killed, so it never runs past the captured
   instant. The node only stops the task once the checkpoint is safely in
   its content store.
3. When the node reports, one transaction deletes the allocation (freeing
   its reservation), marks the snapshot `local`, and sets the instance
   `suspended` with `checkpoint_id`. `instance.suspended` and
   `allocation.removed` with `cause = suspended` are emitted.

**A suspend never stops a workload it failed to capture.** If the capture
fails, the guest is resumed, the instance stays `active`, its allocation keeps
running, and `instance.suspend_failed` carries the node's error. `instance
get` shows it in `detail` until the next successful suspend.

`--checkpoint` overrides the task's `suspend.checkpoint`. Asking for memory
where it cannot work fails before anything is captured:

```text
$ legion instance suspend api-3 --checkpoint memory
Error: instance api-3 cannot take a memory checkpoint: runtime "container" supports "disk" only
Hint: use --checkpoint disk, or run this task with runtime = "sandbox"
```

Suspending an instance that is already suspended, or already being
suspended, changes nothing and names the existing checkpoint. With
`--idempotency-key`, a retry returns the first call's answer with
`deduplicated: true`. Only an `active` instance can be suspended: a `stopped`
or `pending` one fails with `FailedPrecondition`.

## Automatic suspend

Calling `instance suspend` is one way in. Two more put an instance into
`suspended` without anyone asking: the idle policy, when nothing has happened
for a while, and the node itself, when it runs short of memory. Both take the
same checkpoint a manual suspend takes, and `instance get` names which one did
it in `suspend_reason`: `manual`, `idle`, or `pressure`.

### Idle policy

```nickel
suspend = {
  checkpoint = "memory",
  idle = {
    after = "3m",                         # quiet this long, then suspend
    signals = ["network", "cpu", "exec"], # what counts as activity
    cpuBelow = 0.02,                      # the cpu signal's threshold
  },
},
```

| Field | Default | Meaning |
| --- | --- | --- |
| `idle.after` | `3m` | How long the instance must be quiet before it is suspended. At least `30s`. |
| `idle.signals` | `["network", "cpu", "exec"]` | What counts as activity. Any listed signal firing resets the clock. |
| `idle.cpuBelow` | `0.02` | CPU use below this fraction of the task's `cpu` is quiet. Never below 1% of a core, so kernel noise does not keep a sandbox awake. |

The signals:

| Signal | Counts as activity |
| --- | --- |
| `network` | Bytes through the node datapath: connections the task dials, ingress connections to its ports, and a gated task's connections through its egress proxy, counted as the bytes move. On a sandbox, also bytes the guest sends on its own NIC (see below). |
| `connections` | Any open connection through the node datapath, even one that sends nothing: ingress connections to the task's ports, and a gated task's connections through its egress proxy. |
| `cpu` | CPU use at or above `cpuBelow` of the task's `cpu`. |
| `exec` | An open exec session (`legion alloc exec`, an action). Someone working in the instance keeps it awake. |

**The worker detects, the manager decides.** The worker already sees every
signal, so it tracks when each allocation went quiet and reports that
(`idle_since`) with its usage every 10 seconds. The manager owns the instance
row, so it runs a sweep every 15 seconds over those reports, in memory, and
starts a suspend for each instance quiet past `after`. From there it is an
ordinary suspend: the capture, the stop, the freed reservation. Counting the
report interval, the sweep, and the next heartbeat, an idle instance is
suspended within `after` plus about 30 seconds. The sweep interval is the
manager's `suspend.idleSweep`.

`instance get` shows the window while it runs:

```console
$ legion instance get user-42
...
idle since: 2026-10-10T12:00:00Z (suspends after 3m0s)
```

`instance list`'s IDLE column counts the same way for a task with an idle
policy, so it matches what the sweep acts on.

**Why the signals are a list.** A service behind the datapath can hold an
open, mostly quiet connection for a long time, such as a client waiting on a
slow response. Counting only open connections would never suspend it.
Counting only bytes would suspend it in the middle of the wait and break the
stream on resume. The default (`network`, `cpu`, `exec`, three minutes) is
tuned for interactive work that waits on a human. A workload whose quiet
connections must not be cut adds `connections`:

```nickel
idle = { after = "5m", signals = ["network", "cpu", "exec", "connections"] },
```

`connections` sees ingress the node datapath fronts and, for a gated task,
every connection through its egress proxy. So it keeps an agent harness in a
**gated** sandbox awake through a quiet model call, which is an outbound
connection through the proxy, and a long streamed response counts as
`network` activity the whole time it streams. A sandbox that is not gated
makes its outbound connections around the datapath, so there is nothing to
count for a model call. See the next section.

Suspending *during* a model call, and waking when it returns, needs the
workload to say it is parked. That is the `hint` signal, which is not
available yet. A spec that lists it is refused with a hint.

### What each signal sees on a sandbox

A microVM's traffic leaves through its own NIC (a TAP device) or not at all,
and Apple `container` runs the guest in its own VM. So the signals see a
sandbox differently from an `exec` or `container` task:

| Signal | microVM on `none` or `tap` | Gated microVM | Dev mode (Apple `container`) |
| --- | --- | --- | --- |
| `network` | Bytes the guest sends on its TAP device, read from `/sys/class/net/<tap>/statistics/rx_bytes`. Nothing on `none` | Bytes through the egress proxy (and any ingress), as they move | Bytes the container sends, from `container stats` |
| `connections` | **Refused at submit.** No datapath sees the guest's outbound connections (a TAP guest's advertised ports are fronted by the datapath, but counting only inbound would miss a quiet model call) | Every open connection through the egress proxy, and every open ingress connection to the guest's ports, so a quiet model call or a client waiting on a slow response keeps the sandbox awake | **Refused at submit** |
| `cpu` | The VMM's CPU, which is the guest's | The same | The container VM's CPU, from `container stats` |
| `exec` | Open `legion exec` and action sessions over the guest agent's vsock listener | The same | `legion exec` sessions |

Only bytes the guest *sends* count on a TAP or an Apple container. What
arrives includes broadcast and multicast noise on the bridge, which a quiet
guest never asked for and which would keep it awake forever. Any real
exchange, inbound or outbound, has the guest sending something, even if it
is only TCP acknowledgements.

Two rules follow, both checked at submit:

- `connections` on a sandbox that is not gated is refused, with what to do
  instead (run it gated, set `after` longer than the longest quiet call, or
  suspend from your platform):

  ```text
  Error: jobspec failed validation: task "workspace": suspend.idle.signals "connections" counts
  open connections through the node datapath, and this sandbox's outbound traffic never crosses
  it (it is not network.mode = "gated", so it reaches the network on its own NIC or not at all)
  (remove "connections", or run the sandbox gated, where a quiet model call through the egress
  proxy keeps it awake. Otherwise set idle.after longer than the call, or suspend from your
  platform, which knows when a call is in flight) [code=InvalidArgument]
  ```

On a TAP-wired microVM, `network`, `cpu`, and `exec` decide. On `none`,
`cpu` and `exec` do: an open shell keeps the sandbox awake however quiet it
is, because every microVM takes exec sessions through its guest agent
([Exec & Actions](exec-and-actions.md#which-task-runtimes-support-exec)).

The idle policy needs the node to measure activity, which it does only with
`resourceEnforcement.enabled` on the worker type (see
[resource enforcement](resource-enforcement.md#measuring-usage)). On a node
without it the policy cannot act, and `instance get` says so in `detail`
(`idle-unmeasured: ...`).

### macOS dev loop

A Mac has no cgroups, so it measures sandboxes another way. A `sandbox` task
on a macOS worker runs on Apple `container` (dev mode), and with resource
enforcement on (the default) the worker reads each one's CPU, memory, and
network from one `container stats --format json --no-stream` call per usage
report. It confines nothing: on macOS the switch only turns measuring on.
Legion exec sessions (`legion alloc exec`) count for `exec` too.

That is enough for idle suspend in disk mode. A Mac cannot take memory
checkpoints, so the sandbox suspends with `checkpoint = "disk"` and resumes
with a cold boot on its retained volume. The worker needs no configuration:

```nickel
# a dev copy of the sandbox job
{
  name = "agents-dev",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    stateful = { mode = "keyed", requireId = true, storage = { path = "/workspace" } },
    config = {
      image = "alpine:latest",
      command = ["/bin/sh", "-c", "echo boot >> /workspace/boots; exec sleep 100000"],
    },
    resources = { cpu = 100, memory = 256 },
    suspend = {
      checkpoint = "disk",
      idle = { after = "30s", signals = ["network", "cpu", "exec"] },
    },
  }],
}
```

```bash
container system start                 # the Apple container service, if it is down
legion join -a --type devsandbox
legion job run agents-dev.ncl
legion instance create agents-dev --id user-1
legion instance get user-1             # "idle since ..." once a report lands
# ...about after + 30s later: state suspended, suspend_reason idle
legion instance start user-1 --wait    # boots cold on the same /workspace
```

What differs from Linux:

- **Storage is filesystem mode.** Dev mode bind-mounts volumes, so drop
  `mode = "block"` from `stateful.storage`.
- **Disk checkpoints only.** Dev mode cannot capture memory, so a memory
  suspend of a dev-mode sandbox fails and the sandbox keeps running. Keep the
  memory spec for Linux and a disk copy for the Mac.
- **No pressure response.** The node measures, but it has no cgroups to act
  on, so `onPressure` and the pressure steps do nothing.
- **A `container exec` you run by hand is invisible** to the `exec` signal,
  since it bypasses the worker. Its CPU still counts.
- **Two local clusters can share a Mac.** Each dev-mode container is named
  `legion-sbx-<node>-<allocation>` and labeled with its node
  (`legion.node`). Stale-container cleanup removes only a container labeled
  as this node's, so one cluster's cleanup never removes another's
  container.
- **`container stats` is bounded.** The usage sample kills the call past 5
  seconds. A wedged `container` service leaves that report without its
  Apple containers instead of stalling the worker's heartbeat.

The end-to-end test for this loop is
`cargo test --test apple_container_idle -- --ignored`. It needs the
`container` CLI and starts its service when it is down.

### Under memory pressure

```nickel
suspend = { onPressure = true },
```

A node short of memory works through its
[pressure steps](resource-enforcement.md#memory-pressure) in order:
`reclaim`, `suspend`, `evict-bursting`, `evict-reserved`. The `suspend` step
checkpoints and stops the instances whose task sets `onPressure = true`, most
idle first, before anything is evicted. Unlike an eviction it keeps the
instance's state, and it may take an instance with node-pinned storage,
which an eviction never would, because the storage stays where it is.

The node acts on its own here: waiting a heartbeat for the manager's say-so
would be wrong when memory is running out. It takes the same capture a
manual suspend does, under a checkpoint id it mints, and reports it as a
local suspension. The manager applies it with the same transaction, with
`suspend_reason = pressure`, and emits `node.pressure` (step `suspend`) and
`instance.suspended`. `legion node usage` previews it: an opted-in instance's
row reads `reclaim, then suspend`, and no eviction ever follows a suspend.

If the node loses contact with the manager after suspending, it keeps the
checkpoint and re-sends the report until the manager has applied it. If the
instance moved on in the meantime (stopped, destroyed, or replaced on another
node), the manager records the checkpoint for deletion and the node reclaims
it. A capture that fails leaves the task running, and the response moves on
to the next candidate.

## Resuming

```console
$ legion instance start <instance> [--cold] [--wait] [--timeout SECONDS]
```

`start` on a suspended instance moves it to `pending` with its checkpoint as
the restore source, and `instance get` shows it `resuming`. The scheduler
places it:

- **Pinned to the checkpoint's node** while the checkpoint is `local`,
  because it exists only there, the way a
  [node-local volume](volumes.md#node-local-vs-network) pins its instance. A
  `published` checkpoint (`durability = "store"`) is not pinned: see
  [Durability](#durability-surviving-the-node).
- **Checked against the checkpoint's `compat` block** for a memory
  checkpoint: the same VMM version, CPU vendor, model, and features (see
  [Where a restore can run](#where-a-restore-can-run)).
- **As a new allocation.** Allocation ids are never reused, so the resumed
  allocation has a new id, and its workload-identity tokens are issued for
  that id.

The node restores instead of booting, reports the outcome, and the manager
emits `instance.resumed` with the restore time, which `instance get` shows as
the last restore. The spent checkpoint is then reclaimed from the node.

A memory resume loads the guest's device state and lets it run. By default
its memory then loads as the guest touches it, so a resume on the node that
holds the checkpoint takes about the same time at any guest size. A task
whose workload touches most of its memory right after it wakes can load all
of it first instead (`memory_restore = "eager"`, see
[How memory comes back](task-runtime-sandbox.md#how-memory-comes-back)). A
disk resume is a cold boot.

`--cold` discards the checkpoint and boots fresh on the retained storage. It
is the escape hatch when a checkpoint cannot be restored.

### Waking on a request

A task with `suspend.wake` resumes when a request for it reaches
[`legion start gateway`](gateway.md). The gateway calls `StartInstance`,
holds the request until the instance accepts connections on `wake.port`, and
then forwards it, so a sleeping sandbox answers its link a moment later:

```nickel
suspend = {
  idle = { after = "3m" },
  wake = { port = "http", timeout = "30s" },
},
network = { ports = [{ name = "http" }] },
```

A sandbox's port is reachable when it runs gated or on a TAP (see
[Ingress into a sandbox](network-handlers.md#ingress-into-a-sandbox)).
Concurrent requests share one resume. Past `wake.timeout` the client gets
`503` with `Retry-After`. A `stopped` instance is never woken this way. An
[archived](#archiving-idle-instances) instance wakes the same way: its
resume reads the archive manifest back first. `instance get` on a task with
`wake` shows where each port is served while
the instance runs. See [gateway.md](gateway.md) for addressing, `CONNECT`,
the webhook, and errors.

### When a resume cannot restore

A resume can fail to restore because the checkpoint was lost with its node,
the node's VMM or CPU no longer matches, or the restore itself failed on the
node (a corrupt part, a busy TAP device). `suspend.onRestoreFailure` decides:

- **`cold`** (default): boot fresh on the retained storage, emit
  `instance.restore_failed` with the reason and `outcome: cold`, and keep the
  instance usable. The workload's running processes and memory are gone, so
  the instance says so where you look, not only in the event (see below).
- **`fail`**: leave the instance `suspended`, its checkpoint kept, with the
  reason in `detail` and `instance.restore_failed` with `outcome: suspended`.
  `start` on a checkpoint known to be lost fails with `FailedPrecondition`
  and the hint to use `--cold`. This is for platforms that would rather show
  the loss to their user than resume them into a fresh workspace.

A resume also waits at most `suspend.resumeTimeout` (30s by default) to be
placed on its checkpoint's node (any compatible node, for a published
checkpoint). Under `cold`, a resume that waits longer
(the node is down, or full) gives up the checkpoint and boots fresh. A local
checkpoint whose node now runs a different VMM version or CPU settles at once
instead (see [Where a restore can run](#where-a-restore-can-run)). With a
[node-local volume](volumes.md#node-local-vs-network) the fresh boot is still
pinned to that node, so it keeps waiting for it, just no longer for the
checkpoint. Under `fail`, a resume waits like any pending placement.

A cold boot in place of a restore is recorded on the instance as
`cold_boot`, with the snapshot, the reason, and when. It stays until the next
resume or restore starts, or the instance is stopped, so a platform that
checks later still learns the user's session did not survive:

```console
$ legion instance list agents
| id      | ... | state                 | ...
| user-42 | ... | active (resumed cold) | ...

$ legion instance get user-42
...
resumed cold at 2026-10-10T12:03:24Z: restore from snap-91c2a0b3d4e5f601 failed (restore
needs TAP device lgf3c1d...: creating it failed: ...). Running processes were lost, the
volume was kept

$ legion instance get user-42 -j | jq .data.instance.coldBoot
{ "snapshotId": "snap-91c2a0b3d4e5f601", "reason": "restore needs TAP device ...",
  "at": "2026-10-10T12:03:24Z" }
```

`instance start --wait` prints the same `resumed cold at ...` line and returns
`coldBoot` in its `-j` output. The UI's instances card shows a **resumed
cold** badge with the reason, and the MCP tools return the field. An explicit
`start --cold` sets nothing, since that cold boot was asked for. The same
record covers a fork or `from_snapshot` instance whose restore failed, and a
`snapshot restore` rollback whose restore failed.

## Capacity for resumes

Think of a node's memory as a cache over its suspended instances. A
suspended instance gives its reservation back by default, so new work packs
into the room it left. That is the density win. The cost shows up at resume:
a `local` checkpoint can only restore on its own node, and if new work filled
that node, the resume waits.

Legion gives you three controls, from cheapest to strongest guarantee:

- **Make room** (on by default): when a resume is waiting on a full node, the
  manager suspends idle instances there until it fits.
- **Hold a share** (`suspend.reserve`): a
  suspended instance keeps part of its reservation charged to its node, so
  that much room is always waiting for it.
- **Hold everything** (`reserve = true`): the resume always fits, at the cost
  of the whole density win.

`durability = "store"` sidesteps the question: a published checkpoint
resumes on any compatible node. See
[Durability](#durability-surviving-the-node).

### Holding a share of the reservation

```nickel
suspend = {
  reserve = 0.25,                          # a quarter of memory and cpu
  # reserve = true,                        # all of it, exclusive cores included
  # reserve = { memory = "1GiB", cpu = 100 },  # fixed amounts (MiB, MHz)
},
```

| Form | Holds |
| --- | --- |
| `false` (default) or `0` | Nothing. The allocation row is deleted on suspend. |
| A ratio, such as `0.25` | That share of `resources.memory` and `resources.cpu` (MHz). Exclusive `cores`, devices, and custom resources are released. |
| `true` or `1` | The whole reservation, `cores` and devices included. |
| `{ memory, cpu }` | Those amounts. `memory` is a size (`"1GiB"`) or MiB. `cpu` is MHz. Cores and devices are released. |

The charge is the instance's allocation row, kept with status `suspended`
and its resources cut to the held share. The row is never sent to a worker,
but the scheduler's fit check, its read model, and the plan-apply recheck all
count it like any other row, so nothing new packs into the held room. Ports
are released, and a held row does not count toward `maxAllocations`. The
resume deletes the row and places the restore in the same transaction, so
the held share is released at resume and the restore lands on room that
includes it.

A fixed amount must fit inside the task's own reservation, and the dimension
must be declared in `resources`, so submit can check it. A ratio outside 0
to 1 fails the same way. A job update or a job stop removes held rows like
any other allocation, and the instance stays suspended, holding nothing.

`instance get` shows the held share, and `node usage` sums it:

```console
$ legion instance get user-42
...
held: 512 MiB and 250 MHz reserved on w-03 for the resume

$ legion node usage w-03
RESOURCE   CAPACITY      RESERVED         USED            PRESSURE
memory     65536 MiB     41472 MiB (63%)  30120 MiB (46%)  ok (free 33792 MiB, stall 0.2%)
cpu        40000 MHz     21000 MHz (52%)  9300 MHz (23%)

suspended: 12 instances, 3072 MiB held and 1500 MHz (6 holding)
checkpoints: 18.4 GiB suspended here, snapshots budget 19 GiB pinned of 50 GiB (31 GiB free)
```

The held rows also appear in the allocation table as
`<instance> (suspended, held)`.

Which to pick:

- **Interactive fleets** (agent sandboxes, dev environments) where a resume
  should usually be instant: `reserve = 0.25`, and let make-room cover the
  rest. See the [agent sandboxes guide](guides/agent-sandboxes.md).
- **Batch-like work** that tolerates a wait: leave it at `0`.
- **A tier that must resume within its restore time, always**: `true`.

### Making room for a resume

When a resume from a `local` checkpoint cannot fit on its node, the manager
suspends the most idle suspendable instances there until it does. It uses
the same order the node's `suspend` pressure step uses: the least important
band first, then the instance quiet the longest.

An instance can be suspended to make room when all of these hold:

- Its task has a `suspend` block.
- It opted in to being suspended for someone else (`onPressure = true`), or
  it is quiet right now by its own idle policy (`suspend.idle`, with the node
  reporting it idle). An instance with an idle policy that is not quiet is
  left alone.
- Its band does not outrank the resume's band. Making room never displaces
  more important work, the same rule preemption follows.
- It became active (started or resumed) more than two minutes ago.

Work without a `suspend` block is never touched. Suspends start only when
suspending the chosen instances is enough for the resume to fit, counting
what each one keeps held. If even every eligible instance would not be
enough, nothing is suspended.

Two rules stop it from thrashing:

- The two-minute rule above means an instance suspended to make room, once
  resumed, cannot immediately push out the instance it waited for.
- While any suspend is in flight on the node, the manager waits for it to
  land before choosing more. Each resume gets one round every two minutes.

Each round emits `instance.make_room` naming the resume and the instances it
is suspending. Each of those suspends lands as `instance.suspended` with
`reason = make_room`, which `instance get` shows as the last suspend's
reason. Once there is room, the resume is queued for placement right away
rather than waiting for the next evaluation.

The manager checks every 5 seconds. It is on by default. Turn it off in the
manager config:

```nickel
suspend = {
  makeRoom = false,
},
```

Make-room does not change `resumeTimeout`. A resume that still cannot be
placed in time (no eligible instances, or a node that is down) follows
`onRestoreFailure` as before.

### The checkpoint disk budget

Memory checkpoints live in the node content store under the `snapshots`
budget (`content.budgets.snapshots`). By default that is 20% of the disk
holding the worker's state directory, clamped to 2 GiB to 200 GiB (see
[the node content store](artifacts.md#the-node-content-store)). Every
suspended instance's `local` checkpoint stays pinned there until it resumes
or is destroyed, so a node full of suspended instances can run out of room
for the next checkpoint, and a suspend that cannot store its checkpoint
fails. A `published` checkpoint is not pinned on its node (see
[Durability](#durability-surviving-the-node)), so it takes no room the next
checkpoint needs.

The budget is not the only limit. The store also keeps `content.minFreeDisk`
(5 GiB by default) free on its disk: it evicts unpinned content of any class
to stay above that floor, then refuses the write. So a node whose disk is
filled by something else (volumes, logs, images) has no room for a
checkpoint even with budget to spare.

Workers report both on every heartbeat: the budget's size, what it holds,
what is pinned, the disk's free space, the `minFreeDisk` floor, and how much
unpinned content the store could evict. A node's **checkpoint room** is the
smaller of:

- the budget's room: budget minus pinned bytes, and
- the disk's room: free space plus evictable content, minus `minFreeDisk`.

`node usage` shows the room under `checkpoints`, beside the bytes of the
`local` checkpoints suspended there. The manager computes those bytes from
its own records, so the figure survives a worker restart. When the disk is
what limits the room, the line says so with the numbers behind it:

```console
$ legion node usage w-03
...
checkpoints: 18.4 GiB suspended here, snapshots budget 19 GiB pinned of 50 GiB (2.5 GiB free, limited by disk: 6.5 GiB free + 1 GiB evictable - 5 GiB minFreeDisk)
```

The worker page in the UI shows the same room under the `snapshots` bar of
its content card.

When a node's checkpoint room is smaller than one more checkpoint of a
task, the scheduler does not place **new** instances of that task on the
node. This applies only to tasks whose `suspend` block checkpoints memory. A
checkpoint's size is estimated as the guest's memory (`config.memory_mib`,
else `resources.memoryMax`, else `resources.memory`). Resumes and other
restores are never held back, since their checkpoint already exists. When no
node has room, placement fails with a count of which limit was short:

```text
Failed to place task workspace: 4 eligible node(s), but none has 1024 MiB of room for this task's memory checkpoint (suspend): 3 short on the snapshots content budget, 1 short on disk (free space plus evictable content, less content.minFreeDisk). Raise content.budgets.snapshots or free disk on those workers, delete snapshots you no longer need (`legion snapshot list`), or resume or destroy suspended instances to release their checkpoints. `legion node usage <node>` shows each node's checkpoint room
```

A node that has not reported (a manager that just started, or a worker
without a content store) is treated as having room. An instance that failed
to place for lack of room stays `pending` until its job is evaluated again,
like any placement that failed for capacity.

## Durability: surviving the node

A `local` checkpoint lives on one node. A resume has to run there, it waits
when that node is full, and it is gone when that node is. `durability =
"store"` fixes all three by publishing the checkpoint to a shared content
store once it is captured:

```nickel
suspend = {
  checkpoint = "memory",
  durability = "store",
},
```

The worker that captures the checkpoint publishes it to its own
[`contentStore`](artifacts.md#publishing-content) (an S3 bucket, or a shared
directory). Each region's workers point at their own bucket, which is how a
checkpoint stays in its region without any manager-side map:

```nickel
# worker config
contentStore = { url = "s3://legion-content-usw2/" },
```

### What happens

1. The suspend runs exactly as with `local`: the instance becomes
   `suspended`, and its reservation is freed, as soon as the checkpoint is
   written on the node. Capacity never waits on an upload.
2. On its next heartbeat the node seals each part of the checkpoint with a
   key the manager sends it and uploads it to its content store (see
   [Security](#security)). While the upload runs the snapshot is `local`, and
   a resume in that window is pinned to the node like any local checkpoint.
3. When the upload finishes the snapshot moves to `published`, its manifest
   records each part's URL and the key it is sealed under, and
   `snapshot.published` is emitted with the bytes sent and the time it took. `instance list` shows the checkpoint as
   `(memory, published)`.
4. The node unpins its copy of the parts. They stay in its content store,
   but as ordinary cache entries: LRU within the `snapshots` budget, the idle
   TTL, and `minFreeDisk` can evict them to make room for other work. The
   node stops counting the checkpoint as suspended there in `node usage`,
   and its bytes no longer count as pinned against the budget.

From then on:

- **A resume goes to any compatible node.** There is no pin. The manifest's
  `compat` block (VMM version, CPU vendor, model, and features) is checked on
  every candidate, and only workers that advertise the `checkpoint-fetch`
  capability are candidates (every worker since published checkpoints
  shipped does, so an older worker in a mixed fleet is skipped). A node
  that could not run the guest is never chosen,
  and the [`locality` score](artifacts.md#content-locality) prefers a node
  that already holds the parts. With weights otherwise equal, that is the
  node that took the checkpoint, while it still holds them.
- **The new node fetches the parts first.** A part missing from its node
  content store is fetched from its URL through the node's ordinary artifact
  fetchers (`s3://` or `file://`), opened with the key the manager sent with
  the restore, verified against its digest, and stored in the content store
  before the restore starts. The fetch signs with the restoring node's own
  `contentStore.auth` (its KV credential, read at fetch time, or its ambient
  AWS credentials when `auth` is omitted), because the part URL sits under
  that node's `contentStore.url`. Cluster `artifacts.auth` matchers do not
  apply to parts. So every node that may restore the instance needs the same
  `contentStore` url and auth as the node that published it. A node whose
  `contentStore` points elsewhere has no credential for the part and fails
  the restore saying so (see
  [Which credential a reading node uses](artifacts.md#publishing-content)).
  The allocation shows
  `fetching snapshot` meanwhile. This includes the node that took the
  checkpoint, when its copy was evicted: it fetches the parts back like any
  other node. Parts that are present are pinned to the restoring allocation
  until it has started, so they cannot be evicted mid-restore.
- **Losing the node no longer loses the checkpoint.** A resume does not wait
  for a node that is down, and removing a node from the cluster marks its
  `local` snapshots `lost` but leaves `published` ones alone.

```console
$ legion instance suspend user-42 --wait -j
{"ok":true,"data":{"instance":"user-42","state":"suspended",
 "checkpoint":"snap-5b10e2c7a9d04f31","kind":"memory","node":"w-03",
 "durability":"store","checkpointState":"local", ...}}

# w-03 dies. It stops heartbeating and is marked lost.
$ legion instance start user-42 -j
{"ok":true,"data":{"instance":"user-42","state":"pending",
 "resumingFrom":"snap-5b10e2c7a9d04f31", ...}}
# "Resuming instance user-42 from published snap-5b10e2c7a9d04f31 on any compatible node"
```

### The volume has to move too

A checkpoint is only half of a sandbox: the rest is its instance volume. A
suspend leaves that volume in place on its node, so a checkpoint that resumes
elsewhere is only useful if the volume can follow it. Legion does not copy
node-local volumes, so `durability = "store"` needs instance data that is not
pinned to a node:

| `stateful.storage` | `durability = "store"` |
| --- | --- |
| None (everything is in the guest's root disk and memory, which the checkpoint carries) | Works |
| A network driver such as `nfs` | Works: the new node mounts the same volume |
| `local-dir`, `host`, `tmpfs`, or any plugin driver that advertises `nodeLocal` | Refused at submit, and by `instance suspend` |

`job run`, `job validate`, and `job plan` refuse the built-in node-local
drivers. A plugin driver is refused at admission when the cluster's workers
advertise it as `nodeLocal`, and `instance suspend` refuses it again on the
node, so the combination never reaches a capture.

### When publishing cannot happen

- **The node's worker has no `contentStore`.** Such a worker does not
  advertise the `content-publish` capability, and `instance suspend` fails
  with `FailedPrecondition` before anything is captured, naming the node and
  the config block to add. The instance keeps running.
- **The upload fails.** The worker retries each part three times, then
  reports the failure. The snapshot stays `local` with the reason in `error`
  and `snapshot.publish_failed` is emitted, and the manager asks the node to
  publish again one minute later, then five minutes after that. While a
  retry is pending the error reads `publish attempt 1 of 3 failed, retrying
  in 60s: ... access denied`. After the third failure it reads `publish
  failed after 3 attempts: ...` and the manager stops asking. Either way the
  checkpoint keeps working from its node, pinned there like any local
  checkpoint. Fix the store and take a new snapshot (or suspend again) to
  publish after that.
- **The node goes away before the upload finishes.** The snapshot was still
  `local`, so it is `lost`, and `onRestoreFailure` decides what the next
  `start` does.
- **The suspend came from memory pressure.** The node's `suspend` pressure
  step checkpoints on its own and keeps that checkpoint `local`, whatever
  `durability` says: a node short of memory is not asked to upload
  gigabytes too. The next manual or idle suspend publishes as usual.
- **The suspend came from the idle policy on a node that cannot publish.**
  The idle sweep skips the instance (the reason is in the manager's debug
  log) rather than taking a checkpoint it could not publish.

### Warming nodes

A resume on a node that does not hold the parts pays for the download before
the guest starts. `snapshot warm` moves that cost ahead of time:

```console
$ legion snapshot warm snap-5b10e2c7a9d04f31 --constraint region=us-west
Warming snapshot snap-5b10e2c7a9d04f31 on 2 node(s) (1 already warm)
Fetching on: w-05, w-07
Already warm: w-03
Skipped (incompatible): w-09: cpu.model is "6:143:8", the checkpoint needs "6:106:6"
```

Every available node that matches the constraints (node attributes, matched
like a jobspec `constraint` block) and passes the checkpoint's `compat` gets a
`warm` request on its next heartbeat, with the keys for the snapshot's parts.
It fetches and opens the parts it lacks into its node content store and reports `snapshot.warmed` (or `snapshot.warm_failed`
with the reason). The location index then advertises the parts, so placement
prefers those nodes.

Warming is best effort. It never blocks a placement, warmed parts are ordinary
cache entries (LRU and the content store's idle TTL can evict them), and
warming a node that already holds the parts or already has a warm queued does
nothing. Only a `published` snapshot can be warmed. Queued warms live in the
memory of the manager that accepted the call, so a manager restart before the
nodes heartbeat drops them, and you warm again.

`snapshot warm` needs `snapshot:read` on the snapshot's job. It is gated like
a mutating call in the MCP server (`--allow-write`), because it starts
downloads across the fleet.

### Archiving idle instances

A suspended instance with a published checkpoint still holds two rows in the
replicated store: the instance row, and its checkpoint's snapshot row, which
carries the whole manifest (every part's URL, digest, and key). For a fleet
with millions of idle sandboxes, those rows are most of the store, and every
manager keeps a copy. `suspend.archive` moves the checkpoint's record into the
bucket next to its parts once it is published:

```nickel
suspend = {
  checkpoint = "memory",
  durability = "store",
  archive = "slim",               # "slim" | "manifestOnly"
  idle = { after = "3m" },
},
```

| Mode | What stays in the replicated store |
| --- | --- |
| `slim` | The instance row, recording the archive manifest's URL. The snapshot row is dropped. |
| `manifestOnly` | Nothing, for an unnamed instance (created without `--id`) with no `stateful.storage` volume. Any other instance keeps a slim row, as with `slim`. |

`archive` needs `durability = "store"`, a memory checkpoint, and no
`suspend.reserve` (an archived instance holds nothing on any node). Submit
refuses anything else:

```text
Error: task "workspace" sets suspend.archive = "slim", but its checkpoints stay on the node that took them (durability = "local")
Hint: set suspend.durability = "store" so the checkpoint is published to the worker's contentStore before it is archived
```

#### What happens

1. The suspend and the publish run exactly as in [What happens](#what-happens).
2. Once every part is uploaded, the node writes an **archive manifest** to
   `<contentStore.url>/manifests/<job>/<group>/<task>/<instance>.json`: the
   checkpoint's manifest, plus the instance's identity (its id, job, task,
   when it was created and suspended, the fork it came from, and its identity
   epoch).
3. The node's publish report carries the manifest's URL. The manager drops
   the snapshot row, and with `manifestOnly` the instance row, in the same
   entry that records the publish, and emits `instance.archived`.

Archiving writes no raft entry of its own. If the manifest cannot be written,
the publish is reported failed and retried like any failed upload, and the
instance stays an ordinary suspended instance with a `local` checkpoint.

An archived instance is still `suspended`. `instance get` says so, with
`archived: true` and the `manifestUrl`. `instance list` shows the checkpoint
as `(archived)`:

```console
$ legion instance get user-42 -j
{"ok":true,"data":{"instance":{"instanceId":"user-42","state":"suspended",
 "checkpointId":"snap-5b10e2c7a9d04f31","archived":true,
 "manifestUrl":"s3://legion-content-usw2/manifests/workspaces/main/workspace/user-42.json", ...}}}
```

#### Resuming an archived instance

`legion instance start <id>` reads the archive manifest, puts the snapshot
row back in the same entry that starts the resume, and resumes it as a
`published` checkpoint on any compatible node. Once the instance is back in
the store, the manager deletes the manifest. `--cold` does not read it.

An instance archived with `manifestOnly` has no row, so `instance start`,
`get`, and `destroy` look for its manifest in the bucket instead. Its id
names its task (`workspace-7k2qd9x4m1b8c3rj5t0v6wye2h`), so the manager
probes one object per job that has a keyed `workspace` task with
`durability = "store"`, in each `contentStore` the cluster's worker types
name. A start of an id with neither a row nor a manifest is `NotFound`.

When the manifest cannot be read, the resume follows `onRestoreFailure`
exactly as for a lost checkpoint: `cold` boots fresh, `fail` leaves the
instance suspended with the reason in its detail:

```text
instance user-42 cannot resume: checkpoint snap-5b10e2c7a9d04f31 is archived and its manifest s3://legion-content-usw2/manifests/workspaces/main/workspace/user-42.json could not be read (the manifest does not exist)
```

#### Listing archived instances

`instance list` reads rows, so it shows slim archived instances like any
other. `manifestOnly` instances live only in the bucket, and
`--include-idle` lists them from it, a page at a time:

```console
$ legion instance list workspaces --include-idle --idle-page-size 500 -j
{"ok":true,"data":{"instances":[...],"nextIdlePageToken":"0:manifests/workspaces/main/workspace/workspace-7k2q..."}}
$ legion instance list workspaces --include-idle --idle-page-token '0:manifests/...' -j
```

The rows come first on the first page. Every page then adds up to
`--idle-page-size` (default 1,000, at most 10,000) instances from the
bucket, in object-name order across each `contentStore`, and
`nextIdlePageToken` is empty after the last. A page can come back shorter
than its size, because a manifest whose instance also has a row is listed
once, from the row. A bucket instance's state is `suspended`, with
`archived: true`.

#### Stopping and destroying

`instance destroy` and `instance stop` on an archived instance delete its
manifest (destroy of a `manifestOnly` instance deletes only the manifest,
since there is no row). The checkpoint parts stay where they are, like every
published part, until the bucket's lifecycle rule removes them. A lifecycle
rule on `manifests/` is how long an idle `manifestOnly` instance lives: once
its manifest expires, the instance is gone.

The manager reads, lists, and deletes manifests itself, so it needs read,
list, and delete access to every `contentStore`. A bucket whose worker type
sets `contentStore.auth.kv` is reached with that credential, and otherwise
with the manager's ambient credentials (instance role, environment).

## Identity after a resume

A memory restore resumes a guest mid-instruction, under a new allocation, with
a clock that stopped at the capture and an RNG state that other restores of
the same checkpoint could share. Every sandbox launched for memory
checkpoints runs a small resume handler (`legion guest-resume`, the worker's
own binary copied into the guest) that watches the guest's identity disk. On
every restore the node writes a fresh identity there, and the handler:

1. reseeds the kernel RNG with fresh entropy from the node (and VMGenID
   reseeds it too where the VMM exposes it),
2. steps the clock to the node's time,
3. sets the new hostname, and the NIC's MAC when the restore assigns one,
4. writes the new identity where the workload can read it:
   `/legion/identity.json`, `/legion/resume.env` (the refreshed env, including
   `LEGION_ALLOC_ID` and every env-delivered workload-identity token), and one
   file per token under `/legion/identity/`,
5. runs `suspend.onResume` with that env, after all of the above. It has 30
   seconds.

The tokens on the identity disk were issued for the new allocation, so their
`legion.io/alloc` claim names it. Processes already running in the guest keep
the environment they started with, so a long-lived process that needs the new
token re-reads it from `/legion/identity/` or is restarted by `onResume`. The
token the guest held at a suspend's capture is not revoked: it stays valid
until it expires (see [workload identity](workload-identity.md#suspended-and-restored-instances)).
A fork or snapshot capture does retire it in Legion's own verifiers, since
another instance starts with a copy (see
[forks and snapshots](workload-identity.md#forks-and-snapshots)).

The handler logs each step to the guest console, so `legion job logs` shows
what a restore refreshed and what failed. A failed step does not stop the
others.

The same disk refreshes tokens between restores. When the worker re-issues a
workload-identity token (at about half its TTL), it rewrites the running
guest's identity disk in place, with the same allocation and new tokens. The
handler sees a token refresh, not a restore: it rewrites
`/legion/identity/`, `/legion/resume.env`, and any file-delivered token, and
does nothing else. No reseed, no clock step, no `onResume`, and no reboot.
See [workload identity on sandboxes](workload-identity.md#sandboxes).

## Inspecting an instance

`legion instance get <id> -j` (and `instance list -j`, the UI, and
`legion_instance_get`) answers three questions without reading events:

- **Why did it last stop?** `last_stop` names the cause (`node_pressure`,
  `preempted`, `node_lost`, `drain`, `suspended`, `update`, `stop`, and the
  rest of the [stop-cause vocabulary](graceful-stop.md)), a sentence, the
  allocation and node, and when. A memory pressure eviction carries the
  worker's own detail and hint, the same text as its `node.pressure` event,
  so a job-scoped token without `operator:read` still sees it. The manager
  writes it in the same transaction that removes the allocation, and each
  later stop replaces it.
- **What can it checkpoint right now?** For an `active` instance,
  `checkpoint_support` says whether a memory or disk snapshot, a memory
  suspend, a memory or disk fork, a spread fork, and a published snapshot
  would be accepted, and `suspend_default` names the kind a plain `instance
  suspend` takes. Each `*_blocker` is the exact refusal the command would
  return, hint included. The manager runs the same checks the commands run,
  so a stored network policy that gates the task, a node without a VMM, or a
  volume driver without `clone` is already accounted for.
- **Where did it come from?** `forked_from` names the instance a fork was
  forked from, for the fork's whole life (the fork checkpoint itself is
  reclaimed once every fork has restored).
- **Did its last resume keep its running state?** `coldBoot` is set when a
  restore failed and the instance booted fresh on its volume instead (see
  [When a resume cannot restore](#when-a-resume-cannot-restore)).

```console
$ legion instance get user-17 -j | jq .data.instance.lastStop
{ "cause": "node_pressure", "detail": "evicted by evict-bursting on w-03: using 1.9 GiB of a 400 MiB reservation ...",
  "hint": "raise resources.memory toward real usage", "at": "2026-10-10T06:12:44Z", "allocationId": 1063, "nodeId": "w-03" }
```

For the full history, `legion events list --instance <id>` returns every
event about the instance: its suspends, resumes, restores, forks, and
snapshots, plus placements and failures of the allocations that ran it (see
[Event Stream](event-stream.md#events-about-one-instance-or-one-allocation)).

In the UI, every instance id on the job page's Instances table opens the
instance page (`/jobs/<job>/instances/<id>`). It shows:

- the state, mode, slot, current allocation and node, storage volume, and
  fork lineage (forked from, and the forks of this instance);
- the last stop with its cause and hint, and a warning when the last resume
  booted cold;
- memory, CPU, and idle against the reservation, with the idle countdown;
- the checkpoint and what the instance can checkpoint right now;
- the same Suspend, Resume, Resume cold, and Fork controls as the job page,
  plus Stop and Start (Destroy stays CLI-only);
- its snapshots with Restore;
- a live event timeline from the instance filter, grouped by day, with kind
  filter chips.

The timeline needs `operator:read`. Without it the page says so and shows
everything else.

`legion snapshot get <id> -j` adds `holders` for a memory snapshot: every
node that advertises its parts to the manager's location index, with how
many of the parts it holds. That is where a published resume, restore, or
fork will start from local bytes. `legion node content <node>` shows the same
fact from the node's side ([artifacts.md](artifacts.md)).

## Snapshots

The rest of this page covers snapshots you take and restore yourself:
checkpoints to roll an instance back to, rather than to park it.

```console
$ legion snapshot create user-42 --memory --name before-refactor --wait
$ # ...the agent rewrites half the repository...
$ legion snapshot restore user-42 before-refactor --wait
```

## Two kinds of snapshot

| Kind | What it captures | Which tasks | What a restore does |
| --- | --- | --- | --- |
| `disk` (default) | The instance's `stateful.storage` volume, copied with the volume driver's `snapshot` op | Any runtime, when the volume driver supports `snapshot` (the built-in `local-dir` does) | Restores the volume, then starts the task fresh on it |
| `memory` | Guest memory and device state, the guest's root disk, and the instance volume | `sandbox` tasks on Firecracker or cloud-hypervisor that declare a `snapshots` block | Restores the volume, then resumes the guest where it was captured |

A **disk** snapshot is taken while the task keeps running, so it is
crash-consistent: what a power cut at that instant would leave on disk. A
database recovers from it the way it recovers from a crash.

A **memory** snapshot pauses the guest, writes its memory and device state,
copies its root disk, and snapshots the instance volume, all while the guest
is paused. The guest then resumes. Memory, root disk, and volume describe the
same instant, so a restored guest finds its page cache and its disk in
agreement. The pause lasts as long as the VMM takes to write guest memory to
disk, typically under a second for a 1 GiB guest on local NVMe.

A snapshot only ever captures the instance's own `stateful.storage` volume.
Shared `volumes = [...]` claims (a `host` path, an NFS export) belong to more
than one instance and are left as they are.

## The `snapshots` block

```nickel
snapshots = {
  ttl = "7d",       # delete a snapshot this long after it was taken
  keep = 5,         # keep at most this many per instance
  memory = true,    # launch the sandbox ready for memory snapshots
},
```

| Field | Default | Meaning |
| --- | --- | --- |
| `ttl` | unset (never expires) | Duration (`"12h"`, `"7d"`). At least one minute. |
| `keep` | `5` | Taking one more snapshot of an instance deletes its oldest. At least 1. |
| `memory` | `true` | `false` limits the task to disk snapshots. |

Declaring the block is what opts a task into memory snapshots. A `sandbox`
task on a real VMM with the block (and without `memory = false`) launches
ready for checkpoints: Firecracker gets an API socket, the VMM sees its disks
through stable device links, and the guest gets a 1 MiB identity disk (see
[the sandbox runtime](task-runtime-sandbox.md#launching-a-sandbox-for-memory-checkpoints)).
A sandbox without the block launches exactly as it always has, with no control
channel into its VMM.

The block takes effect at launch. Adding it to a running job applies to
allocations started after the change, so restart an instance to make it
memory-snapshottable.

Disk snapshots do not need the block. Without it, a disk snapshot of an
instance whose volume supports `snapshot` works, and it never expires on its
own.

## Taking a snapshot

```console
$ legion snapshot create <instance> [--memory|--disk] [--name NAME] [--publish] [--wait] [--timeout SECONDS]
```

`create` returns as soon as the manager has recorded the snapshot, in state
`capturing`. The instance's node takes it on its next heartbeat (within the
heartbeat interval, 2 seconds by default) and reports back. `--wait` blocks
until it is `local` or `failed` and exits non-zero on failure.

`--publish` gives the snapshot `durability = "store"`: once captured, the node
publishes it to its `contentStore` and it becomes `published` (see
[Durability](#durability-surviving-the-node)). With `--wait`, the command
blocks until the upload settles too, and exits non-zero if it fails. Only a
snapshot made entirely of content-store parts can be published, which today
means a `--memory` snapshot of a sandbox without `stateful.storage`. A disk
snapshot is a copy of the volume inside its driver on one node, so
`--publish` with `--disk`, or with an instance that has a volume, fails with
`FailedPrecondition` and says why.

A name is unique within the job, `[A-Za-z0-9._-]{1,64}`, and cannot start
with `snap-`. Use it anywhere a snapshot id is accepted, together with the
job (`legion snapshot get before-refactor --job workspaces`) or the instance
(`legion snapshot restore user-42 before-refactor`).

Only one capture of an instance runs at a time. A second `create` while one
is `capturing` fails with `AlreadyExists`.

### Snapshot states

| State | Meaning |
| --- | --- |
| `capturing` | Recorded. The node has been asked to capture it. |
| `local` | Captured. The bytes live on `node`. |
| `published` | Captured and uploaded to the capturing node's `contentStore`. Any compatible node restores it, and it survives losing `node`. |
| `failed` | The node could not capture it. `error` says why. The task kept running. |
| `deleting` | Deleted, or expired. The node reclaims the bytes on its next heartbeat. |
| `lost` | Its node was removed from the cluster, and the bytes went with it. |

## Restoring

```console
$ legion snapshot restore <instance> <snapshot> [--wait]
```

A restore rolls a **keyed** instance back to one of its **own** snapshots. The
instance keeps its id and its storage handle:

1. Its current allocation is stopped, with stop cause `restore` (a task can
   read it from `LEGION_STOP_NOTICE_PATH`).
2. The instance moves to `pending`, and `legion instance get` shows the
   snapshot it is restoring from (`restore_snapshot_id` in JSON).
3. The scheduler places a replacement, a new allocation with a new id, on the
   snapshot's node, and checks the node against the snapshot (see below).
4. The node restores the instance volume from the snapshot before mounting
   it. For a memory snapshot it then loads the guest instead of booting it.
5. The node reports the outcome, which emits `snapshot.restored` or
   `snapshot.restore_failed`.

`--wait` blocks until step 5 and exits non-zero on a failed restore.

A restore always uses the instance's **current** job config: its network
policy, mounts, and identity. Nothing from the snapshot widens what the
restored workload may do. The snapshot's `compat` block (VMM and CPU) is the
only snapshot data placement reads.

### Where a restore can run

A `local` snapshot exists on one node only, so its restore is **pinned** to
that node, the way a [node-local volume](volumes.md#node-local-vs-network)
pins its instance. When the node is down or full, the instance waits in
`pending` and the placement error says why.

A `published` snapshot has no pin. Every candidate node is checked against
its `compat` block, the [`locality` score](artifacts.md#content-locality)
prefers nodes holding its parts, and the chosen node fetches what it lacks
before restoring. The same rule applies to every instance that restores from
a snapshot, however it came to restore.

A memory snapshot also records the VMM and CPU it was taken on. A node whose
`sandbox.vmm.<name>.version`, `cpu.vendor`, `cpu.model`, or `cpu.features`
differ cannot resume it, and placement fails with the attribute and both
values:

```text
Failed to place task workspace: instance user-42 restores memory snapshot
snap-3f9a0c21d4e5b6a7, which node w-03 cannot run: cpu.model is "6:143:8",
the checkpoint needs "6:106:6". A memory snapshot resumes only on the CPU and
VMM it was taken on; restore an older snapshot, or restart the instance cold
```

VMM versions compare by release, not by spelling: `v51.2` (what
cloud-hypervisor's `--version` prints) and `51.2.0` (what its API reports)
are the same version, while `51.2.0` and `51.3.0` are not. See
[the sandbox runtime](task-runtime-sandbox.md#where-a-memory-checkpoint-can-be-restored)
for the full rule.

For a published snapshot, placement fails only when no eligible node fits,
and the error names up to three of them with the attribute each one fails on.

A resume from a `local` checkpoint whose node reports a different VMM version
or CPU does not wait out `resumeTimeout`: the checkpoint exists only on that
node, so the manager settles the resume within a second, the same way as a
lost checkpoint. Under `onRestoreFailure = "cold"` the instance boots fresh
with `cold_boot.reason` naming the attribute. Under `"fail"` it goes back to
`suspended` with its checkpoint kept and the reason in `detail`. A node that
has not reported the attribute at all (a fingerprint still arriving) keeps
the resume waiting as before.

The usual cause is a VMM upgrade on the node. Restore a snapshot taken after
the upgrade, or stop and start the instance to boot it cold on its current
storage.

### When a restore fails

A failed restore fails the replacement allocation with the node's reason, and
`snapshot.restore_failed` carries the same text. The instance is no longer
restoring, so the normal [restart and reschedule](task-supervision.md) path
boots it cold on its retained storage. Whether the volume was already rolled
back depends on how far the restore got, and the event says which step
failed.

### Networking

A sandbox's saved state names the host TAP device its NIC was attached to, and
both VMMs reopen that exact name on restore. Before loading the state, the
node makes sure that TAP device is there:

- If the task's current config attaches a different TAP device, or none, the
  restore fails: `restore needs TAP device tap7: the checkpoint's guest is
  wired to it, but the task now names TAP tap9`.
- If the device exists and nothing holds it, the restore uses it.
- If another process holds it (most likely another VM), the restore fails:
  `restore needs TAP device tap7: it exists and another process holds it`.
- If it does not exist, the node creates it under the same `lgf` name,
  attaches it to the task's `bridge`, brings it up, and removes it again
  when the allocation is destroyed, so a resume lands back on the same
  network segment. A failure to create it fails the restore: `restore needs
  TAP device lgf...: creating it failed: ...`.
- A checkpoint wired to a TAP that is not the instance's own (one taken on a
  hand-named `tap = "tap0"` before every TAP was per instance) is moved onto
  the instance's own device, the way a fork's is.

The restored guest also gets an address leased from the bridge's pool on the
restoring node, asking for the one its checkpoint recorded first, and its
resume handler re-addresses `eth0` before `onResume` runs (see
[Addressing](task-runtime-sandbox.md#addressing)). A resume on the same node
normally keeps its address. A resume elsewhere, or one whose old address was
taken meanwhile, gets a new one, which `LEGION_GUEST_ADDR` and `instance get`
show.

A restore never comes up without the network its guest was captured with.

### Gated sandboxes

A gated sandbox (`network.mode = "gated"`, see
[network-handlers.md](network-handlers.md#sandboxes)) has no NIC. Its only
door is a vsock whose host end is `<alloc dir>/.sandbox/vsock.sock`, where
the node datapath listens. Every restore of a gated checkpoint (a resume, a
rollback, a fork, a create from a snapshot, on the same node or another)
moves the guest onto the **restoring allocation's own** socket:

- Firecracker gets `vsock_override` on `PUT /snapshot/load`.
- cloud-hypervisor gets the socket rewritten in the restore's own copy of
  the snapshot's `config.json`, the same rewrite a fork's TAP uses.

The datapath has already bound that allocation's listeners by the time the
restore starts, and it judges every connection by the listener it arrived
on. So the restored guest's traffic is attributed to the new allocation and
judged by the policy in force **now**: a `require` policy applied while the
instance was suspended applies to its first connection after the resume. A
fork is its own allocation with its own socket, so its traffic, denials, and
service-graph edges are its own, and forks cannot collide on the network
because there is no guest address to collide on.

Before resuming the guest, the restore waits up to 5 seconds for the egress
proxy socket (`vsock.sock_1080`). It is normally there already. If it never
appears, the guest resumes anyway with no working door (each connection is
reset), the allocation reports `net_capture_unavailable`, and `legion net
doctor` names the unbound socket. A transient bind error never turns a
resume into a cold boot.

**Advertised ports keep working.** The restoring allocation's ingress
listeners dial the restored guest through its new socket (see
[Ingress into a sandbox](network-handlers.md#ingress-into-a-sandbox)). The
workload in the restored memory still listens on the ports it was captured
with, even when the new placement assigned different numbers, so the
checkpoint records them and the datapath dials those. Consumers keep using
the advertised port the catalog lists for the new allocation. A port the
new placement adds is handed to the guest agent on the identity disk. In
`/legion/resume.env`, `LEGION_PORT_<label>` names the port the restored
workload actually listens on, and `LEGION_ADDR_<label>` the new
allocation's advertised address. A client connected at capture sees its
connection close and reconnects. While the instance is suspended, nothing
serves its advertised port. With `connections` in the idle signals, an open
ingress connection keeps a gated sandbox awake like an open egress one.

**Upstreams keep working.** The workload in the restored memory still dials
the upstream loopback ports it was captured with. The checkpoint records
them, and the restore tells the guest agent which local port the new
allocation was assigned for each, which is where the new allocation's
upstream listener is. The agent relays the old port to it from the first
connection. The agent also listens on the new allocation's VIPs, and keeps
relaying the old ones, so a resolver cache that still holds an old
`<key>.svc.legion` answer works too. In `/legion/resume.env`,
`LEGION_UPSTREAM_<KEY>` and `LEGION_UPSTREAM_PORT_<KEY>` name the loopback
port the restored workload dials, and `LEGION_UPSTREAM_VIP_<KEY>` the new
VIP. An upstream the job added while the instance was suspended gets its
listener in the guest on restore. One the job removed stops working: its
loopback port in the guest stays open, but the new allocation has no
listener behind it.

**Open connections reset.** A memory checkpoint closes every connection the
guest has open through the datapath:

- Firecracker resets the guest's vsock connections when it takes the
  snapshot. That includes a live `snapshot create --memory` and a fork's
  capture, so the **running origin** loses its open connections too, though
  it keeps running. Both commands print a note when this applies:

  ```text
  note: instance user-42 is gated on Firecracker, so this capture resets its open network connections (the guest keeps running)
  ```

- cloud-hypervisor v52 and later reset them in the restored guest.
- Older cloud-hypervisor releases can leave half-open connections in the
  restored guest. The guest agent closes every relay it holds when it sees
  the restore, so a client in the guest gets a prompt close, never a hang.

New connections after the restore work at once. A harness mid-way through a
model call sees the stream end, so retry interrupted calls.

**Firecracker 1.16.1 or later.** Moving the vsock needs Firecracker 1.16.0's
`vsock_override`, and 1.16.1 fixes guest-to-host connections timing out after
a restore, which is exactly the gated datapath's traffic. A memory suspend,
snapshot, or fork of a gated sandbox on an older Firecracker is refused
before anything is captured:

```text
$ legion instance suspend user-42 --checkpoint memory
Error: instance user-42 is gated, and node w-03 runs Firecracker 1.15.0, which cannot move a restored guest's vsock to a new socket (that needs Firecracker 1.16.1 or later)
Hint: upgrade Firecracker on the node, or use --checkpoint disk
```

The sandbox still boots ready for checkpoints there, so upgrading the VMM
and restarting the instance is the whole fix. When neither the call nor the
task's `suspend.checkpoint` names a kind, a suspend on that node takes a
disk checkpoint instead. With `checkpoint = "memory"`, the idle sweep skips
the instance and logs why. The node refuses its own pressure suspend the
same way before pausing the guest. cloud-hypervisor has no floor beyond
Legion's own (v35).

A gated checkpoint restores only into a gated task, and a TAP or
loopback-only checkpoint never into a gated one, since both VMMs restore
exactly the devices they saved. A restore into the other shape fails by
name, and `onRestoreFailure` decides what happens next:

```text
restore needs gated networking: the checkpoint's guest has a vsock datapath and no NIC, but task workspace is now wired to bridge br0; restore a snapshot taken with this network config, or restart the instance cold
```

## Listing, inspecting, and deleting

```console
$ legion snapshot list --instance user-42
SNAPSHOT               NAME             INSTANCE  KIND    STATE  NODE  SIZE      CREATED
snap-3f9a0c21d4e5b6a7  before-refactor  user-42   memory  local  w-03  1.12 GiB  2026-10-10T09:14:03Z
snap-77e0aa41c0de9e12  -                user-42   disk    local  w-03  210 MiB   2026-10-09T17:02:44Z

$ legion snapshot get snap-3f9a0c21d4e5b6a7 -j     # includes the manifest
$ legion snapshot delete snap-77e0aa41c0de9e12
Deleting snapshot snap-77e0aa41c0de9e12
```

A delete moves the snapshot to `deleting`. Its node releases the memory parts
from the [node content store](artifacts.md#the-node-content-store) and runs
the volume driver's `snapshot-delete` op, then the row is removed and
`snapshot.deleted` is emitted. A snapshot whose node is gone, and a failed
one, are removed at once. You cannot delete a snapshot that is still
`capturing`, or one an instance is restoring from right now.

Deleting a `published` snapshot does not delete its parts from the content
store. The same bytes always seal to the same object, so one object can
belong to more than one snapshot, and Legion keeps no count of who else uses
it. Expire them with the bucket's own lifecycle rules (for example, delete
objects under `sealed/` that are older than your longest-lived snapshot).

`snapshots.ttl` and `snapshots.keep` delete through the same path. They never
touch suspend checkpoints, which belong to their instance and go when it
resumes, stops, or is destroyed. Expiry
runs on the manager's maintenance tick, so a snapshot can outlive its `ttl` by
up to one tick. A snapshot an instance is restoring from waits for the
restore to report before it expires. Destroying a job deletes all of its
snapshots.

Destroying an instance keeps its snapshots until they are deleted or expire.

### In the UI

The job page has a Snapshots card for jobs with stateful tasks. It lists
each snapshot's kind, purpose, state, durability, node, size, and expiry,
with its names. A row expands to show its manifest, pause time, and the nodes
holding its parts. The card offers Take snapshot (with publish), Restore,
Name, Warm (published snapshots), and Delete. Restore and Delete ask for a
second click, and so does a memory snapshot, because it pauses the guest. A
disk suspend checkpoint's size reads `in place`: it keeps the volume where
it is and copies nothing.

The Instances card above it filters by state and shows each instance's
checkpoint (kind, local or published, size, node, age), its idle countdown,
the last suspend and restore durations, why it last stopped, and fork
lineage. Keyed instances get Suspend, Resume, Resume cold, and Fork (count
and spread), each a second click to confirm. Memory and spread are offered
only when `checkpoint_support` allows them, and a disabled control's tooltip
is the manager's own refusal.

Both cards follow the same permissions as the CLI: the Snapshots card is
hidden without `snapshot:read`, and buttons are disabled with the reason
when your token lacks `snapshot:write` (create, delete, name, a memory
suspend, fork) or `job:write` (restore, suspend, resume, fork). Creating
instances, including from a snapshot, stays in the CLI. The Events page has
`snapshot` and `instance` categories. See
[ui-architecture.md](ui-architecture.md#usage-pressure-and-snapshots).

## Fork

An agent wants to try three approaches to a problem from the same starting
point. A reviewer wants a copy of a broken workspace to poke at without
touching the original. Fork answers both: it copies a running instance into
new instances that start exactly where the original was, while the original
keeps running.

```console
$ legion instance fork run-17 --count 3 --wait -j
{"ok":true,"data":{"origin":"run-17","snapshot":"snap-c0de4a1b2c3d4e5f",
 "pauseMs":610,"instances":["run-17-f1","run-17-f2","run-17-f3"],
 "kind":"memory","node":"w-03","durability":"local",
 "checkpointState":"local","deduplicated":false}}
```

A fork is three things Legion already does, plus one rule:

1. **A live checkpoint of the origin.** A memory checkpoint pauses the guest
   only while its memory and disks are written, then resumes it. `pause_ms`
   is that window, about 0.5 to 1 second per GiB of guest memory on local
   NVMe. A disk checkpoint copies the instance volume while the task keeps
   running, so it never pauses, and it is crash-consistent like a disk
   snapshot.
2. **New keyed instances that restore from it.** Each fork is a new instance
   of the origin's task. It is not a second allocation of the origin.
3. **Placement next to the bytes.** By default the checkpoint lives on the
   origin's node, so every fork runs there. `--spread` publishes it first and
   puts each fork on its own node (see
   [Spreading forks across nodes](#spreading-forks-across-nodes)).
4. **Every fork is its own instance.** It gets a new id, its own copy of the
   volume, and its own identity, so forks never collide with each other or
   with the origin.

`--checkpoint memory|disk` picks the kind. The default is memory when the
origin can take one (a sandbox launched for memory checkpoints on Firecracker
or cloud-hypervisor) and disk otherwise. A memory fork resumes running
processes. A disk fork boots each fork fresh on its copy of the volume. Both
need `job:write` and `snapshot:write` on the origin's job, since a fork reads
everything the origin held in memory.

### What each fork gets

| | Origin | Each fork |
| --- | --- | --- |
| Instance id | `run-17` | `run-17-f1`, `run-17-f2`, ... (the lowest numbers never used) |
| Allocation | Unchanged, keeps running | A new allocation id |
| Volume | Untouched | Its own copy, made with the volume driver's `clone` op |
| Workload identity | A new [identity epoch](workload-identity.md#forks-and-snapshots): its earlier tokens are refused by Legion and re-issued within a heartbeat | A token issued for the fork's own allocation |
| TAP device (disk fork) | Unchanged | Its own `lgf` device on the task's `bridge` |
| Guest address (TAP sandbox) | Unchanged | Its own lease from the bridge's pool, never the origin's |
| Hostname | Unchanged | `legion-sbx-<allocation>` |
| RNG | Unchanged | Reseeded from fresh entropy |
| MAC (memory fork) | Unchanged | `02:4c:...`, derived from the allocation id |
| TAP device (memory fork with a TAP) | Unchanged | Its own, see [Networking for forks](#networking-for-forks) |

Fork ids are never recycled. When `run-17-f1` was destroyed, the next fork of
`run-17` is `run-17-f2`. An origin id too long to suffix gets server-minted
ids instead. The reply lists the ids either way.

Forks get derived ids under `stateful.requireId = true` too. A fork id is
the caller's id plus `-f<n>`, so it stays in the caller's namespace, and the
caller never has to pick one. There, an origin id too long to suffix (over
124 characters) is refused instead of given a minted id. Create instances
from a snapshot when a platform needs to choose fork ids itself.

The identity refresh is the same one every restore runs (see
[Identity after a resume](#identity-after-a-resume)), with the fork's own
instance id in `LEGION_INSTANCE_ID`. `suspend.onResume` runs in every fork, so
it is the place to drop connections and leases the origin held.

On `local-dir`, the volume copy is a reflink where the filesystem supports
it (btrfs, XFS, APFS) and a sparse copy elsewhere. A write in one fork is
never seen by another fork or by the origin.

### Waiting for the checkpoint

`fork` returns at once with the fork ids. The forks exist right away, in
state `pending` with `pending_op = forking`, and are not placed until the
checkpoint lands. When the node reports the capture, `instance.forked` is
emitted and the forks are placed on the origin's node. `--wait` blocks until
then and fills in `pause_ms`. A spread fork waits for the publish instead.

A fork that does not fit on the origin's node waits in `pending` like any
placement. Each fork is charged its full memory reservation, even though
forks of one memory checkpoint share pages (see below).

When the capture fails, the origin keeps running, `instance.fork_failed`
names the error, and the forks are terminated with the reason in their
`detail`. A fork never boots from nothing.

When a fork cannot restore on the node (its volume clone failed, or its TAP
device is busy), the task's `suspend.onRestoreFailure` decides:

- `cold` (the default) boots the fork fresh on its own volume and records the
  reason in `detail`.
- `fail` stops the fork with the reason in `detail`.

Either way `instance.restore_failed` carries the reason and the outcome
(`cold` or `stopped`).

The fork checkpoint (`purpose = fork`) belongs to its forks. It is reclaimed
once every fork has restored from it, or has been destroyed before it did. It
never counts against `snapshots.keep` and never expires through
`snapshots.ttl`.

A fork stopped before it restored keeps its checkpoint, and keeps waiting for
it if the capture has not landed yet. Starting the fork later restores it from
the checkpoint, never boots it on an empty volume. The checkpoint stays on the
node until the fork restores or is destroyed, so destroy a fork you no longer
want rather than leaving it stopped.

### Spreading forks across nodes

`--spread` puts each fork on a different node, for work that needs more
memory or CPU than one node has, or that should not share a failure domain:

```console
$ legion instance fork run-17 --count 3 --spread --wait -j
{"ok":true,"data":{"origin":"run-17","snapshot":"snap-7e1a90c4b2d35f68",
 "pauseMs":640,"instances":["run-17-f1","run-17-f2","run-17-f3"],
 "kind":"memory","node":"w-03","durability":"store",
 "checkpointState":"published","deduplicated":false}}
```

A spread fork is a fork whose checkpoint is published, the same way
[`durability = "store"`](#durability-surviving-the-node) publishes a suspend
checkpoint:

1. The origin's node captures the memory checkpoint, and the origin keeps
   running. The forks keep waiting.
2. The node publishes the checkpoint to its `contentStore`. When the upload
   lands the snapshot is `published`, `instance.forked` is emitted, and the
   forks are released. `--wait` blocks until here.
3. Each fork goes to a node none of its siblings runs on, among the nodes
   whose worker can fetch published snapshots and whose fingerprint passes
   the checkpoint's `compat`. The origin's node can take one fork. The
   [`locality` score](artifacts.md#content-locality) prefers nodes that
   already hold the parts.
4. Each node fetches the parts it lacks and restores its fork, exactly like a
   published resume.

When there are more forks than nodes that can take one, the rest wait in
`pending` until a node without a sibling has room. The forks of one spread
call never share a node, which is a hard rule like a `distinct` constraint.

A spread fork needs everything a published checkpoint needs:

- **A memory checkpoint.** A disk checkpoint is a copy of the volume on the
  origin's node and cannot be published, so `--spread` takes a memory
  checkpoint and refuses `--checkpoint disk`.
- **No instance volume.** Each fork's copy of the volume is cloned on the
  origin's node, which a fork on another node cannot reach, so an instance
  with `stateful.storage` is refused. Everything such a sandbox has lives in
  its root disk and memory, which the checkpoint carries.
- **A `contentStore` on the origin's worker.** A worker without one is
  refused at the API, naming the config block to add.

A failed publish is retried by the manager like any other (see
[When publishing cannot happen](#when-publishing-cannot-happen)), and the
forks keep waiting through the retries. When the last attempt fails the forks
are terminated with the reason in their `detail`, `instance.fork_failed` is
emitted, and the origin keeps running.

Once every fork has restored, the checkpoint is reclaimed on the origin's
node like any fork checkpoint. Its published parts stay in the content store,
since Legion never deletes from it (see
[Deleting](#listing-inspecting-and-deleting)).

Retrying with the same `--idempotency-key` returns the first call's forks
with `deduplicated: true` and creates nothing.

### Memory sharing

On Firecracker, every fork of one memory checkpoint on a node maps the same
memory file copy-on-write (`MAP_PRIVATE`), through a hard link to the part in
the [node content store](artifacts.md#the-node-content-store). Pages a fork
never writes stay shared in the page cache, so ten forks of a 2 GiB guest
start well under 20 GiB of real memory and grow as they diverge.

Legion still charges each fork its full reservation. Sharing decays as forks
diverge, and placement must not depend on a number that changes under it. The
saving shows up as headroom that [oversubscription](resource-enforcement.md)
and the memory pressure response can use. cloud-hypervisor loads its own
copy of the memory into each guest, all at once or on demand
([How memory comes back](task-runtime-sandbox.md#how-memory-comes-back)), so
its forks share nothing.

### Networking for forks

A fork of a TAP sandbox never shares its origin's device or address. A
sandbox's saved state names the TAP device its NIC was attached to, and a
fork cannot use the origin's, which the origin holds. So a memory fork of a
TAP sandbox gets a TAP device and an address of its own:

- The device is named `lgf` plus 12 hex characters derived from the fork's
  instance id, the name the fork's instance would boot on, and the node
  creates it at restore on the task's `bridge`, brings it up, and removes it
  when the fork's allocation is destroyed. An operator can pre-create it to
  wire it differently, and a free existing device is used as is. A fork that
  later boots cold, or is suspended and resumed, keeps that device.
- Firecracker moves the restored NIC with `network_overrides` on snapshot
  load, which needs Firecracker 1.12 or later. On an older Firecracker the
  fork is refused at the API with a hint (`upgrade Firecracker on the node,
  or use --checkpoint disk`). cloud-hypervisor gets the fork's device through
  the checkpoint's `config.json`, rewritten in the fork's own copy.
- The fork gets a fresh lease from the bridge's pool on its node, never the
  origin's address (see [Addressing](task-runtime-sandbox.md#addressing)).
  The guest's resume handler gives the NIC the fork's MAC, then flushes the
  origin's addresses and applies the lease and its default route, before
  `onResume` runs. So every fork answers on its own address, with no
  network configuration in the image.

A fork's later checkpoints record its own TAP and address, so suspending and
resuming a fork keeps them.

A disk fork boots each fork fresh on its own `lgf` device with its own
lease, so both kinds of fork work on a TAP sandbox.

### Refusals

Fork refuses what it cannot do, at the API, before creating anything:

| Error | Why |
| --- | --- |
| `has no running allocation (it is stopped)` | Only an `active` instance forks. Start it first, or create instances from a snapshot |
| `has a suspend in flight` | Wait for the suspend or resume to finish |
| `has no stateful.storage volume, so a disk fork has nothing to copy` | `exec` and `container` fork only through a snapshot-capable volume |
| `does not support \`clone\`` / `does not support \`snapshot\`` | The instance volume's driver cannot copy the volume per fork |
| `supports "disk" only` | A memory fork of a runtime other than `sandbox`. Use `--checkpoint disk` |
| `was not launched for memory checkpoints` | Add a `suspend` or `snapshots` block and restart the origin, or use `--checkpoint disk` |
| `is gated, and node w-03 runs Firecracker 1.15.0, which cannot move a restored guest's vsock` | A memory fork of a gated sandbox needs Firecracker 1.16.1 or later. Upgrade it, or use `--checkpoint disk` |
| `runs Firecracker 1.11.0, which cannot move a restored NIC` | Upgrade Firecracker to 1.12 or later, or use `--checkpoint disk` |
| `count 65 is above the limit of 64` | Fork in smaller batches |
| `a disk fork boots each fork fresh on the task's configured TAP` | A disk fork of a TAP-wired sandbox would share the origin's TAP. Use `--checkpoint memory` |
| `--spread places forks on other nodes, ... a disk checkpoint ... cannot be published` | Drop `--checkpoint disk`, or drop `--spread` |
| `--spread places forks on other nodes, but instance ... keeps its data on a \`local-dir\` volume` | A spread fork cannot carry an instance volume. Drop `--spread`, or `stateful.storage` |
| `--spread publishes the fork checkpoint ... whose worker has no contentStore` | Add `contentStore { url }` to the origin's worker config |
| `is too long to fork ... requireId = true` | The fork id would pass 128 characters. Use shorter ids, or create from a snapshot |

## Create from a snapshot

A platform keeps a warmed-up "golden" sandbox (dependencies installed,
models loaded, language server indexed) and starts every new user from it.
That is a snapshot plus `--from-snapshot`:

```console
$ legion snapshot create builder-1 --memory --name agent-golden --wait
$ legion instance create agents --from-snapshot agent-golden --count 50
Created 50 instance(s): agents-...
Each restores from snap-91c2a0b3d4e5f601
```

Every new instance restores from the snapshot exactly like a fork does: its
own copy of the volume, its own identity, its own MAC and TAP for a TAP-wired
memory snapshot, and following `onRestoreFailure` when the restore fails. A
`local` snapshot pins every instance to the snapshot's node and is checked
against its `compat` there. A `published` one (`snapshot create --publish`)
places each instance on any node that can fetch it and passes its `compat`,
so one golden snapshot can seed a whole fleet. The snapshot
is untouched and can seed any number of later creates. `--from-snapshot`
works with `--id` too, and needs `snapshot:read` on the job as well as
`job:write`.

The snapshot must be a `local` or `published`, `manual` snapshot of the same
job and task. A suspend checkpoint belongs to its instance, and a fork
checkpoint to its forks, so neither can seed new instances. A memory snapshot
seeds only a task with the same network shape: a gated snapshot into a gated
task, a TAP or loopback one into a task without gated networking. The other
way round fails each restore by name (`restore needs gated networking: ...`),
and `onRestoreFailure` decides. One call creates at most 64 instances.

### Named snapshots

`--name` on `snapshot create` names a snapshot. `snapshot alias` names one
after the fact, moves a name to a newer snapshot, or removes it:

```console
$ legion snapshot alias set agents agent-golden snap-5b1e0f9a7c3d2e18
Moved agent-golden from snap-91c2a0b3d4e5f601 to snap-5b1e0f9a7c3d2e18
$ legion snapshot alias delete agents agent-golden
Removed agent-golden (it pointed at snap-5b1e0f9a7c3d2e18)
```

A weekly rebuild takes a new snapshot and repoints the name. Creates after
that use the new snapshot. Moving a name never touches the snapshot it moves
away from, and instances already restoring from it are unaffected. A name
resolves at call time, so `create` returns the snapshot id it used in
`restoringFrom`.

Names are unique within a job, `[A-Za-z0-9._-]{1,64}`, and never start with
`snap-`. Only a `local` or `published` `manual` snapshot can be named. A snapshot can carry
several names, and `snapshot get` lists them under `Names` (`aliases` in
JSON). Deleting a snapshot removes its names.

## How it works

The manager owns each snapshot's state machine. The node that holds the bytes
does the work. They talk over the heartbeat:

- Every heartbeat reply carries a `CheckpointRequest` for each snapshot of
  that node in `capturing` (`op = capture`) or `deleting` (`op = delete`).
  The manager re-sends it until it has applied the node's answer.
- The worker runs each request once and answers with a `SnapshotReport` on its
  next heartbeat. When it sees a request again for work it already finished,
  it re-sends the same report instead of redoing the work. That makes a lost
  heartbeat, a worker restart, or a report that landed on a manager that
  could not apply it a retry rather than an outage.
- An allocation that restores from a snapshot carries the snapshot's manifest
  in its dispatch payload (`TaskAllocation.restore`). The worker reports the
  restore's outcome the same way, and the manager clears the payload once it
  has it. When the snapshot was taken from another instance (a fork, or a
  create from a snapshot), the payload says so (`fork = true`) and names the
  new instance. The worker then clones the snapshot's volumes into the
  allocation's own volumes instead of rolling them back, and gives the guest
  its own MAC and TAP device.
- A fork is a `purpose = fork` snapshot plus forks waiting on it with
  `pending_op = fork`. The scheduler neither counts nor binds a waiting
  fork. The capture report releases them in the same transaction that makes
  the checkpoint `local`, and enqueues their placement.
- A `local` snapshot with `durability = "store"` is publish work for its
  node (`op = publish`) until the node reports the upload. A queued warm is
  `op = warm` work for its node until the node reports the fetch.

The manager sends checkpoint work only to workers that advertised the
`checkpoint` capability when they registered. A request for an instance on an
older worker fails with `FailedPrecondition` naming the worker's version.

On the node, a memory capture writes the VMM's memory and state files into the
content store's staging directory, then moves each one into the store's
`snapshots` class, pinned to the snapshot id and verified against the digest
the capture recorded. A restore checks that each part is present at its
recorded size and hardlinks it into the new allocation. It does not rehash
the part: the store checked the digest when the bytes came in and keeps
them read-only, and rehashing would read all of the guest's memory before
it could start. A `local` snapshot's pinned parts are
never evicted, and they count toward `content.budgets.snapshots`. A capture
that would go over the budget fails with an error naming it. Once a
snapshot is published, the node drops its pins and the parts become
evictable cache entries. Deleting a snapshot removes its parts from the
capturing node either way.

A disk capture is the volume driver's `snapshot` op. For `local-dir` that is a
reflink on btrfs, XFS, and APFS, and a sparse copy elsewhere. The copy lives
beside the volume and survives the volume's destroy.

A publish seals each memory part from the node content store and uploads it
to `<contentStore.url>/sealed/<name>`. The name and the key both derive from
the part's digest and the cluster's checkpoint key, so publishing bytes the
store already holds sends nothing. A cross-node restore fetches each
missing part on the allocation's staging thread, never under the worker's
task-manager lock, and pins it to the allocation until the restore has
started. The parts then stay in the node content store as ordinary cache
entries.

## Security

- **`snapshot` is its own ACL resource.** `snapshot:write` covers create and
  delete. `snapshot:read` covers list, get, and restore (a restore also needs
  `job:write`). Scope rules with the job-name and namespace selectors, like
  `job` rules. See [auth and ACLs](auth-and-acls.md).
- **A memory snapshot holds everything the workload had in memory**, secrets
  and tokens included. Granting `snapshot:write` on a job is equivalent to
  granting read access to its in-memory secrets.
- **Local snapshot bytes stay on the node.** Memory parts are read-only files
  owned by the worker inside its state directory.
- **Published parts are sealed.** Every part a worker publishes
  (`durability = "store"`, `--publish`, and `--spread` forks) is encrypted
  with AES-256-GCM before it leaves the node. The bucket holds only
  ciphertext, under names that do not reveal what the part contains. There
  is no setting that publishes plaintext. See
  [Published checkpoint encryption](#published-checkpoint-encryption).
- **Restores refresh identity, and a resume keeps what was captured.** Every
  restore (a resume or a snapshot restore) runs as a new allocation, and the
  guest's resume handler picks up a token issued for it (see
  [Identity after a resume](#identity-after-a-resume)). A suspend checkpoint
  only ever resumes the same instance, so the token the guest held at capture
  stays valid until it expires, and a checkpoint taken less than one TTL ago
  contains a live token. A manual snapshot can seed other instances, so its
  capture retires the instance's earlier tokens in Legion's own verifiers
  (see [workload identity](workload-identity.md#forks-and-snapshots)). Keep
  workload-identity TTLs short on tasks you suspend or snapshot.
- **A memory suspend needs `snapshot:write`** as well as `job:write`, for
  the same reason as a memory snapshot. A disk suspend needs only
  `job:write`, like `instance stop`. Resume is `job:write`.
- **Fork needs `snapshot:write` and `job:write`** on the origin's job, for
  either kind: the forks start from the origin's memory or disk. Creating
  instances from a snapshot needs `snapshot:read` and `job:write`. Naming a
  snapshot needs `snapshot:write`.
- **Forks get their own identity, but share what the origin knew.** Every
  fork gets its own token, hostname, MAC, and entropy. The token the origin
  held at capture is in every fork's memory too, so a fork or snapshot
  capture moves the origin to a new
  [identity epoch](workload-identity.md#forks-and-snapshots): Legion refuses
  the origin's earlier tokens from then on and re-issues the origin a fresh
  one within a heartbeat. Relying parties that verify tokens offline (AWS,
  GCP, Vault) cannot see the epoch, so there the copied token works until it
  expires. Keep TTLs short on tasks you fork. Secrets the origin loaded, ACL
  tokens it logged in for, open connections, and leases are copied too. Use
  `suspend.onResume` to drop them.
- **Keep API keys out of the guest entirely.** A key the workload never
  holds cannot be captured, forked, or published. For HTTP APIs, declare it
  on an `https://` upstream with
  [credential injection](network-handlers.md#credential-injection): the
  node sets the header on the way out, from KV or as a workload-identity
  token, and the value lives only in the worker's memory and on the TLS
  connection to the origin. Rotation is a `legion kv put`, and no checkpoint
  taken before or after it contains the key. This reaches `exec`,
  `container`, and gated `sandbox` tasks; a TAP-wired or loopback-only
  sandbox cannot reach its upstreams.

## Published checkpoint encryption

A memory checkpoint holds every secret the guest had loaded. So a published
part never leaves the node in the clear, and the bucket alone is not enough
to read one. This needs no configuration. It uses the same keyring that
encrypts [KV values](kv-encryption.md).

### How the keys work

```
root key          outside the store, as for KV (file, env, or a KMS)
   │ wraps
checkpoint key    ck_…, a replicated keyring row, minted with the keyring
   │ derives, per part digest
part key          sent by the manager to the one worker doing that part's
                  publish, warm, or restore, and held in memory only
```

- **The manager holds the checkpoint key.** Workers never see it. A worker
  receives one key per part, only with work the manager sent it: a publish
  for a snapshot it captured, a warm accepted through `snapshot warm`
  (`snapshot:read`), or a restore placed through resume, `snapshot restore`,
  fork, or `instance create --from-snapshot`, each checked against its own
  ACL first. A worker cannot ask for a key.
- **Each part is sealed in 1 MiB chunks** (AES-256-GCM, with every chunk
  bound to its position and to the part's length). A part of any size is
  sealed and opened with one chunk in memory. Opening happens in place, so
  a fetch needs no disk beyond the downloaded object.
- **The same bytes seal to the same object**, so a re-publish uploads
  nothing and identical parts are stored once. Both the key and the object
  name are keyed by the checkpoint key, so without it nobody can tell what a
  part contains, or test a guess against it. Someone who can list the bucket
  can still see object sizes, and that one object is shared by two
  snapshots.
- **The manifest records which key** sealed each part (`keyId`, beside
  `url`). `legion snapshot get` shows it. Key bytes never appear in a
  manifest, a log line, an event, or the worker's state file.

### Rotating and losing keys

- **`legion keyring root-key rewrap`** rewraps the checkpoint key with the
  data keys. Published objects are untouched and keep working.
- **`legion keyring rotate`** rotates KV data keys only. The checkpoint key
  never rotates on its own, so no published object ever has to be
  re-encrypted.
- **A lost root key loses every published checkpoint**, like every KV value.
  Keep the root key with your backups ([KV encryption](kv-encryption.md)). A
  `legion keyring reset` drops the old checkpoint key and mints a new one.
  Snapshots published before the reset can no longer be fetched, and their
  restores fail by name (below).
- **A backup** carries the wrapped checkpoint key with the rest of the
  keyring. Restore it with its root key and published checkpoints keep
  working.

### When a part cannot be opened

Every case fails the publish, the warm, or the restore with a named reason,
and a restore then follows `onRestoreFailure`. A part this node already holds
needs no key, so a resume on the node that took the checkpoint works even
while the manager's keyring is locked.

| Error | Meaning |
| --- | --- |
| `failed authentication at chunk N: it was modified, or sealed under a different key` | The object was changed in the bucket, or belongs to another cluster. Not retried |
| `truncated or has trailing bytes` | The object is cut short or has bytes appended |
| `is not a sealed checkpoint part (no LGNSEAL1 header)` | A plaintext object, for example one published before this release. Not retried |
| `was published before checkpoint encryption, and Legion no longer reads unsealed parts` | The manifest has a `url` and no `keyId`. Take a new snapshot |
| `this cluster has no checkpoint key yet` | The keyring leader has not minted it. It does within seconds of initializing |
| `this cluster has no checkpoint key, because the keyring is off` | `encryption.enabled = false` on the managers. Publishing needs the keyring |
| `keyring locked: checkpoint key ck_… cannot be unwrapped` | The manager cannot reach its root key. See [KV encryption](kv-encryption.md) |
| `checkpoint key ck_… is not in this cluster's keyring` | A `keyring reset` dropped it, or the snapshot came from another cluster |

### Limits

- **Object sizes are visible.** Sealing adds 20 bytes plus 16 bytes per MiB.
- **Identical parts share one object**, and the bucket shows that two
  snapshots share it. Only the checkpoint key turns that into knowing what
  the part is.
- **The checkpoint key cannot be rotated by hand yet.** A root-key rewrap
  covers the usual rotation need.
- **The node copy is not encrypted.** A part in a worker's own content store
  is a `0444` file in its state directory, like the rest of the worker's
  data.
- **Publishing needs the KV keyring.** With `encryption.enabled = false` on
  the managers and no checkpoint key from before, `durability = "store"`
  publishes fail by name. The checkpoint stays `local` and keeps working.

## Events

| Kind | Level | When |
| --- | --- | --- |
| `snapshot.created` | info | A capture finished and the snapshot is `local`. |
| `snapshot.failed` | warn | A capture failed. The metadata carries `error`. |
| `snapshot.deleted` | info | A snapshot's row is gone (delete, `ttl`, `keep`, or job destroy). |
| `snapshot.restored` | info | An allocation started from a snapshot. |
| `snapshot.restore_failed` | warn | An allocation failed to start from a snapshot. The metadata carries `error`. |
| `snapshot.published` | info | A `durability = "store"` snapshot finished uploading and is `published`. The metadata carries `uploadedBytes` and `publishMs`. |
| `snapshot.publish_failed` | warn | A `durability = "store"` snapshot could not be uploaded and stays `local`. The metadata carries `error`, which says whether the manager will retry. |
| `snapshot.warmed` | info | A node fetched a published snapshot's parts after `snapshot warm`. The metadata carries `fetchedBytes` and `durationMs`. |
| `snapshot.warm_failed` | warn | A node could not warm a snapshot. The metadata carries `error`. |
| `instance.suspended` | info | A suspend's capture landed. The instance is `suspended`. The metadata carries `kind`, `totalBytes`, `durationMs`, `reserved` (whether it holds a share of its reservation), and `reason` (`manual`, `idle`, `pressure`, or `make_room`). |
| `instance.archived` | info | A published suspend checkpoint was archived (`suspend.archive`). The metadata carries `snapshotId`, `manifestUrl`, and `manifestOnly` (whether the instance row was dropped too). |
| `instance.make_room` | info | A resume did not fit on its checkpoint's node, and the manager started suspends to make room. The metadata carries the resume's `instanceId` and the instances it is `suspending`. |
| `instance.suspend_failed` | warn | A suspend's capture failed. The instance keeps running. The metadata carries `error`. |
| `instance.resumed` | info | A resume restored as a new allocation. The metadata carries `restoreMs`. |
| `instance.restore_failed` | warn | A resume, a fork, or a create from a snapshot could not restore. `outcome` is `cold` (booted fresh, also recorded as the instance's `cold_boot`), `suspended` (a resume under `onRestoreFailure = "fail"`), or `stopped` (a fork or new instance under `"fail"`), and `error` says why. |
| `instance.forked` | info | A fork's checkpoint landed (for a spread fork, was published) and its forks are being placed. The metadata carries `origin`, `instances`, `pauseMs`, and the checkpoint's `kind` and `totalBytes`. |
| `instance.fork_failed` | warn | A fork's capture failed, or a spread fork's publish failed for good. The origin keeps running and the forks in `instances` were terminated. The metadata carries `error`. |

Each carries `snapshotId`, `instanceId`, and the job, node, and allocation
scopes. The suspend also emits `allocation.removed` with `cause = suspended`.
A suspend by the node's pressure response also emits `node.pressure` with
`step = suspend`, `cause = suspended`, and the `snapshot_id`. See
[the event stream](event-stream.md).

## Metrics

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `legion_instance_suspend_seconds` | histogram | `kind` | How long each successful suspend's capture took on its node. |
| `legion_instance_resume_seconds` | histogram | `kind` | How long each successful resume's restore took on its node. Placement time is not included. |
| `legion_checkpoint_bytes` | histogram | `kind` | Size of each captured checkpoint (suspends and snapshots). A disk suspend reports 0, since its volume stays in place. |
| `legion_instance_suspends_total` | counter | `reason` | Suspends applied, by `manual`, `idle`, `pressure`, or `make_room`. |
| `legion_make_room_suspends_total` | counter | `outcome` | Make-room suspends: `started`, `failed` (could not start), or `no_room` (a round that would not have made enough room, so nothing was suspended). |
| `legion_idle_sweep_suspends_total` | counter | `outcome` | Suspends the idle sweep tried to start: `started`, or `skipped` (the instance changed, an operation was in flight, or the node cannot checkpoint). |
| `legion_checkpoint_publish_bytes` | histogram | `kind` | Bytes a node uploaded to publish one checkpoint. 0 when the store already held every part. |
| `legion_checkpoint_publish_seconds` | histogram | `kind` | How long each successful publish took on its node. |
| `legion_instance_archived_total` | counter | `mode` | Instances archived after their checkpoint published, by `slim` (a slim row stays) or `manifestOnly` (no row stays). |
| `legion_checkpoint_publish_total` | counter | `outcome` | Publishes reported, by `published`, `retrying` (failed, the manager asks again), or `failed` (the last attempt failed). |

See [metrics](metrics.md#suspend-and-resume).

## Failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| `create` fails with `worker version X ... upgrade the worker` | The instance's node runs a worker without the `checkpoint` capability | Upgrade the worker |
| `create --memory`: `supports disk snapshots only` | The task is not a `sandbox` | Use `--disk` |
| `create --memory`: `was not launched for memory checkpoints` | No `snapshots` block, or `memory = false`, when the allocation started | Add the block and restart the instance |
| `create --memory`: `without Firecracker or cloud-hypervisor` | The sandbox runs on a dev backend (Apple `container`, an OCI runtime) | Use `--disk` |
| `create`: `does not support snapshots` | The instance volume's driver does not advertise the `snapshot` op | Use a driver that does, such as `local-dir` |
| `create --disk`: `has no stateful.storage volume` | Nothing to copy | Declare `stateful.storage` |
| Snapshot `failed`: `memory snapshots need the node content store` | `content.enabled = false` on the worker | Enable the content store |
| Snapshot `failed`: `...budget...` | `content.budgets.snapshots` is full of pinned snapshots | Delete snapshots, lower `keep`, or raise the budget |
| Instance stuck `pending` after a restore | The snapshot's node is down or full, or its CPU or VMM no longer match | Read the placement error in `legion job info` or the eval events |
| `snapshot.restore_failed`: `restore needs TAP device ...` | The recorded TAP device is busy, different, or could not be created | See [Networking](#networking) |
| `instance suspend`: `no running allocation (it is stopped)` | Only an `active` instance can be suspended | `instance start` it first |
| `instance suspend --checkpoint memory`: `supports "disk" only` | The task is not a `sandbox` | Use `--checkpoint disk` |
| `instance suspend`: `was not launched for memory checkpoints` | No `suspend` or `snapshots` block when the allocation started | Add the block and restart the instance, or use `--checkpoint disk` |
| `instance.suspend_failed` | The capture failed on the node (content store disabled or full, the VMM API failed) | Read `detail` or the event's `error`. The instance keeps running |
| Instance stuck `pending (resuming)` | The checkpoint's node is down or full, and nothing there can be suspended to make room | Wait, `instance start --cold`, or let `resumeTimeout` boot it cold. To keep room for resumes, see [Capacity for resumes](#capacity-for-resumes) |
| `legion_make_room_suspends_total{outcome="no_room"}` rising | Resumes wait on nodes whose instances are not eligible to make room (no `suspend` block, not opted in, a more important band, or active under two minutes) | Set `onPressure = true` or an idle policy on the work sharing those nodes, or hold a share with `suspend.reserve` |
| Placement fails: `none has ... MiB of room for this task's memory checkpoint` | Every eligible node is short on its `snapshots` budget (pinned `local` checkpoints fill it) or on disk (free space plus evictable content is under `minFreeDisk` plus one checkpoint). The message counts each | Raise the budget, free disk, delete old snapshots, or resume or destroy suspended instances. `legion node usage <node>` shows which limit binds |
| Resume of a `published` checkpoint fails: `fetching part ... from its content store` | The node's copy was evicted after publishing (or it never had one), and it cannot read the `contentStore`. The fetch uses the restoring node's own `contentStore.auth` (or its ambient AWS credentials when `auth` is omitted), not `artifacts.auth` | Give the node a `contentStore` with the publisher's url and an `auth` that can read the bucket, or fix the store's reachability. The checkpoint itself is intact |
| Resume fails: `this node has no credential for content store s3://...` | The part was published to a store other than the restoring node's own `contentStore.url` (another region's bucket, a different prefix or endpoint, or no `contentStore` at all), so it was fetched anonymously and refused | Give the node the same `contentStore` url and auth as the publishing node, or constrain the resume to nodes that have it |
| Submit: `suspend.reserve ... is out of range` or `exceeds the task's resources` | A ratio outside 0 to 1, or an amount larger than the reservation | Follow the hint |
| `instance start`: `cannot resume: checkpoint ... is lost` | The node holding it was removed, and `onRestoreFailure = "fail"` | `legion instance start <id> --cold` |
| `instance list` STATE `active (resumed cold)`, `cold_boot` set | A restore failed (lost or incompatible checkpoint, `resumeTimeout`, an error on the node) and `onRestoreFailure = "cold"` booted it fresh. Its running processes are gone, its volume is kept | Read `cold_boot.reason`. Use `onRestoreFailure = "fail"` if a lost session must never look like a resume |
| `detail: idle-unmeasured: ...` | The node sends no usage reports, so nothing measures activity | Set `resourceEnforcement.enabled` on the worker type |
| An idle instance never suspends | A listed signal keeps firing (an open `exec` session, a client holding a connection with `connections` listed, background CPU above `cpuBelow`) | Compare `instance get`'s idle since with what the task is doing, then adjust `signals` or `cpuBelow` |
| `legion_idle_sweep_suspends_total{outcome="skipped"}` rising | The sweep found idle instances it could not suspend | The manager's debug log names why (a worker without the `checkpoint` capability, a memory checkpoint the node cannot take) |
| Submit: `suspend.idle.signals "hint" is not implemented yet` | The workload's own idle signal is not available | Remove `hint`. Add `connections` to keep quiet datapath connections awake (ingress on any runtime, and egress on gated tasks, sandboxes included) |
| Submit: `"connections" counts open connections through the node datapath, and this sandbox's outbound traffic never crosses it` | A sandbox that is not gated has no datapath connections | Remove `connections`, or run the sandbox gated. Otherwise raise `after` or suspend from your platform |
| A TAP-wired sandbox never suspends | The guest keeps sending on its TAP (NTP, ARP, a chatty agent) | Compare `node usage`'s IDLE column with the guest's traffic, or drop `network` from `signals` |
| Allocation fails: `sandbox needs TAP device lgf... on bridge br0: it exists and another process holds it` | The instance already runs a guest on this node | Stop the other allocation first |
| Allocation fails: `bridge br0's sandbox range ... is full (N addresses leased on node w-03)` | Every address in the node's `sandbox.bridges.br0.range` is leased | Widen `range` in the worker config, or stop sandboxes on that node. See [Addressing](task-runtime-sandbox.md#addressing) |
| A TAP sandbox stays `pending` with `no available nodes matching constraints` | No node declares its bridge in `sandbox.bridges` (the implicit `sandbox.bridges contains` constraint) | Add `sandbox.bridges.<bridge>` to the worker type that should run it |
| A TAP guest boots with no address | The guest kernel lacks `CONFIG_IP_PNP`, so it ignores `ip=` | Use a kernel built with `CONFIG_IP_PNP=y` |
| Submit: `suspend.idle.after "5s" is shorter than 30s` | Reports every 10s and a 15s sweep cannot honor it | Use `30s` or longer |
| `instance fork` refused | See [Refusals](#refusals) | Follow the hint in the message |
| Forks stuck `pending (forking)` | The origin's node has not reported the capture yet (it is down, or busy writing memory) | Wait, or watch the checkpoint with `legion snapshot get <snapshot>` |
| Forks `terminated` with `fork of ... failed` | The capture failed. The origin kept running | Read the forks' `detail` or `instance.fork_failed`, fix the cause, and fork again |
| Forks stuck `pending` after `instance.forked` | The origin's node is full, or (spread) every node that can take one already runs a sibling | Free capacity on the node, add nodes, or fork fewer |
| A spread fork stays `pending (forking)` after its capture | Its checkpoint is still being published, or a publish retry is pending | `legion snapshot get <snapshot>` shows the publish error and when it retries |
| A restored or forked TAP guest answers on its old address, or none | The resume handler's `address` step failed, and the guest console logs why (for one, a workload that renamed `eth0`) | Read the allocation's console log. Legion's init never renames the NIC, so leave it `eth0` |
| Submit: `config.network.tap is gone` or `config.network.mac is gone` | The job names a TAP device or MAC by hand, which Legion now owns | Set `bridge` to the bridge the device was on, drop `tap` and `mac`, and declare `sandbox.bridges.<bridge>` on the worker type |
| `instance create --from-snapshot`: `is a fork checkpoint` or `suspend checkpoint` | Only manual snapshots seed instances | Take one with `legion snapshot create` |
| Submit fails: `sets suspend.durability = "store", but its stateful.storage uses volume driver ...` | The instance volume cannot leave its node | Use a network driver, drop `stateful.storage`, or keep `durability = "local"` |
| `instance suspend`: `whose worker has no contentStore to publish to` | `durability = "store"` on a node without a `contentStore` block | Add `contentStore { url }` to that worker's config |
| `snapshot.publish_failed`, snapshot `local` with `publish attempt N of 3 failed, retrying in ...` | The upload failed (credentials, bucket, network), and the manager will ask again | Fix the store before the retry. The checkpoint still works from its node |
| Snapshot stays `local` with `publish failed after 3 attempts: ...` | Every attempt failed | Fix the store, then take a new snapshot or suspend again |
| `snapshot.restore_failed`: `... is not on this node and was never published` | A restore of a local snapshot reached a node that does not hold it | Use a published snapshot when the restore has to run elsewhere |
| `snapshot.restore_failed`: `fetching part ... from its content store` | The new node could not fetch a published part (no access to the bucket with its `contentStore.auth` or ambient credentials, a scheme not in `artifacts.fetch.allowedSchemes`) | Give the node read access to the store through its own `contentStore` |
| `snapshot.restore_failed` or `snapshot.publish_failed` naming a sealed part, a checkpoint key, or the keyring | The part could not be sealed or opened | See [When a part cannot be opened](#when-a-part-cannot-be-opened) |
| `snapshot warm`: `is local, not published` | Only published snapshots can be warmed | Publish with `durability = "store"` or `--publish` |
| Submit: `sets suspend.archive = ... but its checkpoints stay on the node` | `archive` needs a published checkpoint | Set `durability = "store"` |
| Submit: `sets suspend.archive = ... and suspend.reserve` | An archived instance holds nothing on a node | Remove `reserve`, or remove `archive` |
| `instance start`: `checkpoint ... is archived and its manifest ... could not be read` | The archive manifest was deleted (a bucket lifecycle rule, by hand) or the manager cannot read the bucket | With `onRestoreFailure = "cold"` it boots fresh. Otherwise give the manager read access, or `instance start --cold` |
| `instance start` or `get` of a `manifestOnly` instance: `Unavailable ... could not read the archive manifest` | The manager could not reach a `contentStore` to look for it | Give the manager read and list access to every `contentStore` |
| An archived instance's `snapshot.published` arrives, but no `instance.archived` | The instance changed before the publish landed (it was started, stopped, or suspended again), so it was left as is | Nothing: the checkpoint is still published |

## Limitations

- **Local snapshots stay on their node.** A `local` snapshot cannot be
  restored anywhere else, and losing the node loses it (`lost`). Only a
  `published` one moves.
- **Only content-store parts can be published.** A volume snapshot (every
  disk snapshot, and the volume half of a memory snapshot) stays in its
  driver on one node, so a published checkpoint needs instance data that is
  not node-local. Legion does not copy `local-dir` volumes between nodes.
- **A publish is retried only three times.** The manager asks again after
  one minute and after five more. After the third failure the snapshot stays
  `local` for good. Take a new snapshot (or suspend again) once the store is
  fixed. There is no command to retry by hand.
- **Published parts are not deleted from the store.** Deleting the snapshot
  removes the row and the node's local copy only. Use bucket lifecycle rules.
- **Published part sizes are visible in the bucket, and identical parts
  share one object.** See
  [Published checkpoint encryption](#published-checkpoint-encryption).
- **Archive manifests outlive some of their instances.** `instance stop`
  and `destroy` delete an archived instance's manifest, but `job stop` and
  `job destroy` do not: a slim instance's manifest is left orphaned in the
  bucket, and a `manifestOnly` instance stays there, startable again once
  the job runs (a job destroyed and submitted again under the same name does
  not resume them, because the job's creation time is checked). Expire
  `manifests/` with a bucket lifecycle rule.
- **A `manifestOnly` id is only as unique as its 130 random bits.** There is
  no row to refuse a duplicate, which is why only unnamed instances drop
  their row: a caller id needs a row to stay unique.
- **The manager needs bucket access.** Reading, listing, and deleting archive
  manifests happen on the manager, with the worker type's
  `contentStore.auth.kv` credential or the manager's ambient credentials.
  A manager that cannot reach a bucket can neither resume nor list what is
  archived in it.
- **A node reads published parts only from its own `contentStore`.** It signs
  a part fetch with its own `contentStore.auth` only when the part URL is under
  its own `contentStore.url`, and there is no per-store credential list for
  reading other stores. Placement does not check which store a node is
  configured for, so a resume can land on a node pointed at another bucket
  and fail with `this node has no credential for content store`. Give every
  node that may restore a checkpoint the publisher's `contentStore`, or
  constrain the job to those nodes.
- **`--include-idle` pages can come back short.** A manifest whose instance
  also has a row is listed once, from the row, so a page can hold fewer than
  `--idle-page-size` bucket instances. Page until `nextIdlePageToken` is
  empty.
- **Warms and the location index are held by one manager.** In a
  multi-manager cluster, nodes that heartbeat a different manager than the
  one that accepted `snapshot warm`, or that scheduled the placement, are
  invisible to it until they heartbeat it.
- **Rollback is in place and keyed-only.** `snapshot restore` rolls back only
  the instance a snapshot was taken from. To start a new instance from it,
  use `instance create --from-snapshot`. Slot instances can be snapshotted
  but not restored or forked, since their lifecycle belongs to the scheduler.
- **Forks run on the origin's node unless spread.** A fork checkpoint is
  node-local, so forks without `--spread` all run there and wait when it is
  full. `--spread` needs an instance with no `stateful.storage` volume and a
  worker with a `contentStore`. Instances created from a `local` snapshot run
  on its node for the same reason.
- **Spreading applies to a fork's first placement.** A spread fork that is
  later rescheduled (its node lost, or stopped and started) is placed like
  any instance and can land next to a sibling.
- **Forks copy the guest's in-memory secrets.** Legion gives each fork its
  own MAC, TAP, address, hostname, token, and entropy, but open connections
  and anything the origin had in memory come along. `onResume` is where a
  workload resets them.
- **Each fork is charged its full reservation**, even though forks of one
  memory checkpoint share pages on Firecracker. cloud-hypervisor forks share
  no pages.
- **A published checkpoint is fetched whole before it restores.** A resume
  on a node that holds the parts runs the guest before its memory is read,
  but a node that must fetch them downloads every part first. Warm the node
  ahead of time (`snapshot warm`) to keep that off the resume.
- **A lazily resumed guest pays for its memory as it touches it.** See
  [How memory comes back](task-runtime-sandbox.md#how-memory-comes-back) for
  when to set `memory_restore = "eager"`.
- **A fork's TAP needs Firecracker 1.12 or later** for a memory fork of a
  TAP-wired sandbox. A disk fork of a TAP-wired sandbox is refused, since it
  would boot on the origin's TAP.
- **A stopped fork holds its checkpoint.** A fork stopped before it restored
  keeps the checkpoint on the node until it is started (and restores) or is
  destroyed.
- **Memory snapshots need a `sandbox` on Firecracker or cloud-hypervisor**,
  with the same VMM version, CPU, and guest size at restore. A gated sandbox
  on Firecracker needs 1.16.1 or later.
- **A published Firecracker checkpoint moves only between nodes with the
  same data dir.** Firecracker reopens a restored guest's disks at the
  device-link paths it captured, under `<data dir>/vmdev/`, so a node with
  another `LEGION_DATA_DIR` refuses the restore by name. cloud-hypervisor
  checkpoints have no such limit. See
  [Where a memory checkpoint can be restored](task-runtime-sandbox.md#where-a-memory-checkpoint-can-be-restored).
- **A memory checkpoint closes a gated guest's open connections.** On
  Firecracker that includes the running origin of a live snapshot or a fork.
  New connections work at once. See [Gated sandboxes](#gated-sandboxes).
- **Gated restores have not run on a real VMM yet.** The vsock move on both
  VMMs is tested against fake VMM APIs that check the requests Legion sends,
  not against Firecracker or cloud-hypervisor themselves.
- **The worker task-manager lock is held while the VMM writes guest memory.**
  Heartbeats from that node wait for it. For a guest of a few GiB on slow disk
  this can approach the heartbeat interval.
- **A memory part is hashed twice**, once when captured and once when stored,
  which adds seconds for a multi-GiB guest after it has resumed.
- **The resume handler needs a guest binary that runs on the image.** The
  static `legion-guest` runs on any image. A worker without it copies itself,
  and a guest whose libc cannot run that (a musl image when the worker is
  built against glibc) restores without refreshing its identity, and init
  says so on the console. See
  [The guest agent](task-runtime-sandbox.md#the-guest-agent).
- **Suspend checkpoints are local by default.** With `durability = "local"`
  a suspended instance resumes only on the node that holds its checkpoint.
  Losing the node loses the checkpoint, and `onRestoreFailure` decides what
  happens next. `durability = "store"` lifts this once the upload finishes,
  except for a suspend the memory pressure response took, which stays local.
- **The idle policy needs resource enforcement.** Activity is measured from
  the allocation's cgroup (on macOS, `container stats`) and the node
  datapath, so a node without `resourceEnforcement.enabled` never suspends
  for idleness. On macOS only dev-mode sandboxes are measured, and only
  disk suspend works.
- **Idle is coarse.** Activity is sampled every 10 seconds and swept every
  15, so `after` is honored to within about 30 seconds, and a burst shorter
  than a sample interval counts only through bytes or an open session, not
  CPU.
- **Only connections through the datapath count as `connections`.** A port
  the task binds directly on the host (no `ingress` chain) is invisible to
  the node, so its connections and bytes do not keep the instance awake. CPU
  and the egress datapath still do. A sandbox that is not gated has no
  datapath connections at all, so `connections` is refused there.
- **A sandbox's NIC counts bytes, not connections.** On a TAP, a quiet
  outbound connection from the guest (a harness waiting on a model) sends
  nothing, so it looks idle. Run the sandbox gated and add `connections`, or
  set `after` longer than the longest quiet call.
- **`connections` counts any open connection**, including an idle keepalive
  the workload holds to some service. A gated sandbox that keeps one open
  never suspends while it is open, which is why `connections` is not in the
  default list.
- **No `hint` signal yet.** A workload cannot say "parked until this call
  returns", so suspending during a model call is not possible.
- **A pressure suspend is lost if the worker restarts before reporting it.**
  The report is held in memory and re-sent across lost heartbeats, but a
  worker restart in that window leaves the checkpoint pinned on the node with
  no record, and the node starts the allocation again, fresh, on its
  retained storage.
- **`exec` and `container` suspend to disk only**: process memory is not
  kept.
- **A held share is fixed when the suspend lands.** Changing a job's
  `suspend.reserve` does not re-charge instances already suspended.
- **Make-room reads usage held by one manager.** Like the idle sweep, it
  sees only nodes that heartbeat to the manager running it, and it knows an
  instance is quiet only from nodes that send usage reports
  (`resourceEnforcement.enabled`). Without reports, only `onPressure = true`
  makes an instance eligible.
- **Make-room does not preempt.** It only suspends, so a resume on a node
  full of work without a `suspend` block still waits.
- **The checkpoint budget is soft state.** It lives in the manager's memory,
  like the content location index. Right after a manager starts, and for a
  node that never reports one, the scheduler assumes there is room. The
  check is per placement and estimates a checkpoint at the guest's full
  memory size, so a plan placing many instances at once can still overfill a
  budget that had room for one.
- **The disk half of the room check is a heartbeat old.** Free space is
  read when the worker builds its report, so a disk that fills between two
  heartbeats is seen on the next one. Evictable content counts as room, but
  an evicted blob that a running restore still hardlinks frees nothing, so
  the room can be slightly overstated.
- **A resume is not checked for room.** A `published` checkpoint resuming on
  a node that lacks its parts must fetch them, and that fetch can fail on a
  node with no room left. The placement does not yet look for a node with
  room for the parts.
- **Share budgets are sized at worker start.** Growing or shrinking the disk
  under a running worker changes nothing until the worker restarts.

## Reference

| CLI | RPC | MCP tool | Mutates |
| --- | --- | --- | --- |
| `legion snapshot create` | `CreateSnapshot` | `legion_snapshot_create` | yes |
| `legion snapshot list` | `ListSnapshots` | `legion_snapshot_list` | no |
| `legion snapshot get` | `GetSnapshot` | `legion_snapshot_get` | no |
| `legion snapshot delete` | `DeleteSnapshot` | `legion_snapshot_delete` | yes |
| `legion snapshot restore` | `RestoreInstance` | `legion_snapshot_restore` | yes |
| `legion snapshot warm` | `WarmSnapshot` | `legion_snapshot_warm` | yes (starts downloads) |
| `legion instance suspend` | `SuspendInstance` | `legion_instance_suspend` | yes |
| `legion instance start [--cold]` | `StartInstance` | `legion_instance_start` | yes |
| `legion instance fork` | `ForkInstance` | `legion_instance_fork` | yes |
| `legion instance create --from-snapshot` | `CreateInstance`, `CreateInstances` | `legion_instance_create` (`from_snapshot`) | yes |
| `legion snapshot alias set` | `SetSnapshotAlias` | `legion_snapshot_alias_set` | yes |
| `legion snapshot alias delete` | `DeleteSnapshotAlias` | none | yes |

Every mutating call takes `--idempotency-key` (`idempotency_key` over gRPC
and MCP) and reports `deduplicated`. The full contract is in the proto
comments in `proto/legion.proto`.
