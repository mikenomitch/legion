# Running Agent Sandboxes on Legion

An agent platform gives each user a sandbox: a microVM with a shell, a
repository, a dev server, and a harness driving a model. Most of those
sandboxes are idle at any moment, because the user stepped away or the agent
is waiting on them. An idle sandbox still holds its whole memory reservation,
so idle memory is what a fleet of sandboxes costs you.

Legion has no "agent sandbox" object. The guide composes general pieces
instead: keyed instances, burstable memory, memory checkpoints, suspend and
resume, an idle policy, and a memory pressure response. Each one is documented
on its own page, and this guide shows how they fit together for a platform
team running thousands of sandboxes. It covers the worker setup, the spec and
why each part is there, the platform's lifecycle, capacity, security, and what
happens when things go wrong.

Runnable spec: [`configs/jobs/agent-sandboxes.ncl`](../../configs/jobs/agent-sandboxes.ncl).

## Run it

```bash
legion job validate configs/jobs/agent-sandboxes.ncl
legion job run configs/jobs/agent-sandboxes.ncl
legion instance create agents --id user-42
legion instance get user-42
```

The job is keyed, so `job run` places nothing. Each `instance create` boots
one user's microVM. From there, the sandbox suspends itself after three quiet
minutes, and `legion instance start user-42` brings it back where it left off.

Memory checkpoints need Linux with KVM and Firecracker or cloud-hypervisor on
the worker, so the spec places on Linux workers only. A Mac can validate it,
and runs a disk-checkpoint copy of it for development. See
[The macOS dev loop](#the-macos-dev-loop).

## Set up the workers

Three worker settings matter for sandboxes: resource enforcement, which
measures what each sandbox uses and caps it, an oversubscription ratio, and
the content store, where memory checkpoints live. Put sandboxes in a pool of
their own, so the ratio and the pressure response apply to them and not to
your databases.

```nickel
# configs/workers/sandbox.ncl
{
  workerType = "sandbox",
  pool = "sandboxes",

  resourceEnforcement = {
    # Required. Measures usage (which the idle policy reads), sets
    # memory.low and memory.max per sandbox, and turns on the pressure
    # response.
    enabled = true,
    cgroupParent = "legion",
    # Schedulable memory = (physical - reservedResources.memory) x 1.25.
    # See "Picking a ratio" below before you raise it.
    oversubscribe = { memory = 1.25 },
    memoryPressure = {
      freeBelow = "4GiB",
      stallAbove = "10%",
      settle = "5s",
      # Keep `suspend` in the list. It is the step that keeps a sandbox's
      # state when the node runs short.
      steps = ["reclaim", "suspend", "evict-bursting", "evict-reserved"],
    },
    # Keep a quarter of every suspended sandbox's reservation on its node,
    # so most resumes find room without waiting. See "Room for resumes".
    suspendedReserve = 0.25,
  },

  # The VMM's own memory on top of the guest's.
  sandbox = { vmmOverheadMiB = 64 },

  # Memory checkpoints are pinned in the `snapshots` class until the
  # instance resumes or the snapshot is deleted. Size it for the
  # checkpoints you expect to park on one node (see "Capacity").
  content = {
    enabled = true,
    budgets = { artifacts = "20GiB", snapshots = "500GiB" },
    minFreeDisk = "20GiB",
  },
}
```

**`resourceEnforcement.enabled`** is not optional here. Without it the node
sends no usage reports, so the idle policy cannot see that a sandbox went
quiet, `memoryMax` is not enforced, and there is no pressure response. It
needs cgroup v2 and a root worker. A node that cannot enforce logs a warning
and runs tasks unconfined. See
[resource enforcement](../resource-enforcement.md#turning-it-on).

**`memoryPressure`** works with no block at all. Write one when you want to
tune the trigger or check that `suspend` is in `steps`. The block above
mostly restates the defaults. See
[memory pressure](../resource-enforcement.md#memory-pressure).

**`suspendedReserve = 0.25`** keeps a quarter of each suspended sandbox's
memory and CPU charged to its node. See [Room for resumes](#room-for-resumes).

**`content`** is on by default with a 50 GiB snapshot budget. That is enough
to try the guide and too small for a node that parks hundreds of sandboxes.
A suspend whose checkpoint would go over the budget fails, and the sandbox
keeps running. Once a node's budget has no room for one more checkpoint, the
scheduler places new sandboxes elsewhere, and `legion node usage` shows the
budget under `checkpoints`. See
[the checkpoint disk budget](../suspend-and-resume.md#the-checkpoint-disk-budget)
and [the node content store](../artifacts.md#the-node-content-store).

### Room for resumes

A suspended sandbox's checkpoint lives on its node, so its resume has to run
there. By default a suspended sandbox frees its whole reservation, new
sandboxes pack into that room, and a returning user can find their node full.
Two things keep that rare:

- **Make-room** is on by default. When a resume waits on a full node, the
  manager suspends the quietest sandboxes there (every sandbox in this guide
  has an idle policy and `onPressure = true`) until it fits. The user waits
  for one capture, usually under a second.
- **`suspendedReserve`** keeps part of every suspended sandbox's reservation
  in place, so the room is usually already there.

For interactive fleets, start with `suspendedReserve = 0.25`. With 512 MiB
reservations a suspended sandbox holds 128 MiB, so a node gives up a quarter
of the density win in exchange for resumes that rarely need make-room. Lower
it toward 0 if `node usage` shows held memory crowding out active sandboxes.
Raise it, or set `suspend.reserve = true` on a premium tier's job, if users
notice waits. See
[Capacity for resumes](../suspend-and-resume.md#capacity-for-resumes).

### Picking a ratio

Start at 1.0. Most of the density in this design comes from suspend, which
frees a sandbox's reservation entirely. A fleet of 2,000 users with 150
active reserves memory for 150 sandboxes, whatever the ratio. The ratio only
matters when active sandboxes alone would fill your nodes.

When you do raise it, measure first:

1. Run at 1.0 through a few busy days and read `legion node usage` on your
   busiest nodes at peak. Compare `RESERVED` with `USED`.
2. Pick a ratio that keeps peak `USED` well under physical memory, with
   `freeBelow` to spare. If nodes are 90% reserved and 45% used at peak, a
   ratio of 2.0 would fill physical memory at peak. 1.5 leaves headroom, and
   1.25 is the safer start.
3. Raise it in steps of 0.25 and watch `node.pressure` events and
   `legion_node_pressure_actions_total{step}`. A `suspend` step at every peak
   means the ratio is too high for your users.

**What a ratio above 1.0 gives up.** At 1.0, a sandbox within its 512 MiB
reservation is never stopped for node pressure. Above 1.0, reservations can
add up to more than physical memory, so that guarantee is gone, and the
`evict-reserved` step is allowed to run. A sandbox with node-local storage is
never evicted, because it could only come back on the same node. So on a
sandbox pool, the steps that can act on a sandbox are `reclaim` and
`suspend`. If both fall short, the kernel OOM killer is the backstop, and an
OOM kill takes the sandbox's in-memory state with it. Its `/workspace`
survives. See [oversubscription](../resource-enforcement.md#oversubscription).

Changing the ratio takes a worker restart, because the worker reports it at
registration.

## The spec, and why each part is there

```nickel
{
  name = "agents",
  priority = "normal",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    group = "workspaces",

    stateful = {
      mode = "keyed",
      idSource = "caller",
      storage = { path = "/workspace", mode = "block", params = { size = "10G" } },
    },

    config = {
      image = "ghcr.io/acme/agent-base:2026-10",
      command = ["/usr/local/bin/harness"],
      network = { mode = "tap", tap = "auto", bridge = "br0" },
      vcpus = 2,
    },

    resources = { cpu = 250, memory = 512, memoryMax = 4096 },

    suspend = {
      checkpoint = "memory",
      reserve = false,
      onRestoreFailure = "cold",
      resumeTimeout = "30s",
      onResume = ["/usr/local/bin/harness", "reconnect"],
      durability = "local",
      idle = { after = "3m", signals = ["network", "cpu"], cpuBelow = 0.02 },
      onPressure = true,
    },

    snapshots = { ttl = "14d", keep = 3, memory = true },

    identities = [{
      name = "platform",
      audience = ["api.acme.dev"],
      ttl = "15m",
      file = { path = "/legion/identity/platform" },
    }],

    constraints = { os = "linux" },
  }],
}
```

**`runtime = "sandbox"`** runs each instance in its own microVM, with its own
kernel. That is the isolation boundary you want for code an agent wrote. It is
also the only runtime that can take a memory checkpoint. `exec` and
`container` tasks suspend to disk only. See
[the sandbox runtime](../task-runtime-sandbox.md).

**`stateful.mode = "keyed"`** makes each sandbox an instance the platform
creates and destroys by name. Submitting the job places nothing, and `count`
doesn't apply. An instance keeps its id, its volume, and its checkpoint across
reschedules and suspends. Ids are never recycled, so a destroyed user's data
can't leak into a new instance that reuses the name. See
[keyed mode](../stateful-workloads.md#keyed-mode-the-management-api).

**`idSource = "caller"`** makes `--id` required. Your platform already has
user or session ids, so let them be the instance ids rather than mapping
server-minted UUIDs back to users.

**`storage` with `mode = "block"`** is the part of the sandbox that outlives
everything except `instance destroy`. A guest kernel attaches a block device,
so sandbox storage is block mode. The default `local-dir` driver keeps it on
the node and pins the instance there. A suspend leaves the volume in place,
and a cold boot finds it intact. Anything outside `/workspace` lives in the
guest's root disk, which a memory checkpoint carries and a cold boot rebuilds
from the image. See [volumes](../volumes.md#node-local-vs-network).

**`resources.memory = 512`** is the reservation. The scheduler packs on it,
and the cgroup protects it with `memory.low`, so the kernel reclaims from
other sandboxes above their reservations before it touches this one. Set it
near what an idle-but-awake sandbox uses, which `legion node usage` measures
for you.

**`resources.memoryMax = 4096`** is the ceiling, and it sizes the guest: the
VM boots with 4 GiB of RAM. A build or a test run can burst to it while the
node has room. Everything above 512 MiB is what the `reclaim` pressure step
gives back first, through the guest's balloon. The cgroup's hard limit is
`memoryMax` plus the worker's `sandbox.vmmOverheadMiB`. See
[burstable memory](../resource-enforcement.md#burstable-memory-memorymax) and
[guest size](../task-runtime-sandbox.md#guest-size).

**`resources.cpu = 250`** is in MHz, and it does two jobs. The scheduler
packs on it, and the cgroup turns it into a CPU weight that only matters when
the node's CPUs are contended. Keep it low for idle-heavy sandboxes, or CPU
fills the node before memory does: a 64,000 MHz node fits 64 sandboxes at
1000 MHz, and 256 at 250. An active sandbox still bursts to whatever CPU is
free. `cpus` is an alias for this same MHz number, not a core count, so
`cpus = 1` asks for 1 MHz, and `job run` warns about any `cpu` or `cpus` under
10. Use `cpu` and avoid the confusion.

**`config.vcpus = 2`** is the guest's shape: how many vCPUs its kernel sees.
It is not a reservation. The host weights the VMM's threads by
`resources.cpu`, so both vCPUs share that one weight. A memory checkpoint
records `vcpus` and the guest size, and a restore into a task whose shape
changed is refused. Treat both as fixed for the life of a checkpoint.

**`suspend.checkpoint = "memory"`** captures the running VM: guest memory,
device state, and the root disk. A resume continues the processes, the shell
history, and the warm caches. `"disk"` would stop the VM and boot it fresh on
`/workspace` later. Declaring the block also launches each sandbox ready for
checkpoints, with a VMM API socket, stable device links, an identity disk,
and a balloon. A sandbox started before you added the block can't be
memory-checkpointed until it restarts.

**`reserve = false`** frees the job's claim on the reservation while the
sandbox is suspended. That is the density win. The node's
`suspendedReserve` still holds its floor. A ratio (`0.25`) or a fixed amount
(`{ memory = "256MiB" }`) holds a share for this job alone, and `true` keeps
everything so a resume always fits, which costs the whole point. See
[Room for resumes](#room-for-resumes).

**`onRestoreFailure = "cold"`** decides what happens when a checkpoint can't
be restored. A user who comes back to a fresh boot on their own `/workspace`
is better off than one locked out of it. Use `"fail"` when your platform would
rather show the loss and let the user choose. **`resumeTimeout`** is how long
a resume waits for its checkpoint's node before `cold` gives up the
checkpoint.

**`onResume`** runs in the guest after every memory restore, once the
guest's hostname, clock, RNG, and identity have been refreshed. Use it to
reconnect the harness to your platform, renew a DHCP lease, or drop leases the
captured process held. It has 30 seconds.

**`durability = "local"`** keeps the checkpoint on the node that took it, so
a resume runs there. It is the default, written out so a reader knows a node
loss loses the checkpoint. See [Surviving a node loss](#surviving-a-node-loss).

**`idle`** suspends the sandbox once it has been quiet for `after`. Three
minutes suits a human who stepped away. The signals are the two a TAP-wired
microVM can show: bytes the guest sends on its TAP (`network`) and its CPU.
`exec` is left out because a microVM takes no exec sessions, and
`connections` is refused for a sandbox that isn't gated. Read
[An instance never goes idle](#an-instance-never-goes-idle-or-goes-idle-too-soon)
before you tune them. See
[idle policy](../suspend-and-resume.md#idle-policy).

**`onPressure = true`** lets a node that runs short of memory suspend this
sandbox, state intact, before it evicts anything. See
[under memory pressure](../suspend-and-resume.md#under-memory-pressure).

**`snapshots`** opts into on-demand snapshots on top of suspend: a rollback
point before an agent rewrites the repository, or a warmed "golden" sandbox
to start new users from. `ttl = "14d"` gives a weekly golden snapshot a week
of overlap. See [snapshots](../suspend-and-resume.md#snapshots).

**`identities`** gives the harness a platform token at
`/legion/identity/platform`, refreshed at about half its 15-minute TTL with
no reboot. See [Security](#security).

**`config.network`** gives each sandbox a NIC of its own, and the next
section explains it.

### Networking

`tap = "auto"` has the node create a TAP device for each instance and attach
it to `br0`. The device is named `lgf` plus 12 hex characters of the
instance id's hash, so `user-42` always asks for the same device. A resume
recreates it under the name its checkpoint recorded, on the same bridge, and
destroying the allocation deletes it. A fork of `user-42` gets its own `lgf`
device on `br0`, the one it would boot on. Any number of auto-TAP sandboxes
fit on a node, and every one can take memory checkpoints.

The bridge is yours: create `br0` on each sandbox node, route or NAT it, and
run DHCP on it, since every sandbox boots from the same image. Each guest
gets a MAC derived from its allocation id, so guests on one bridge never
collide. The worker needs `CAP_NET_ADMIN` and the `ip` tool. Whatever
filtering you want between sandboxes and the outside goes on the bridge.

The options, side by side:

| `network` | Guest gets | Memory checkpoints | Sandboxes per node |
| --- | --- | --- | --- |
| none (`config.network.mode = "none"`, the default) | Loopback only | Yes | Any number |
| `config.network = { mode = "tap", tap = "auto", bridge = "br0" }` | A NIC on a TAP Legion creates per instance, on your bridge | Yes | Any number |
| `config.network = { mode = "tap", tap = "tap0" }` | A NIC on a host TAP device you create | Yes | One per task, since the device name is fixed in the spec |
| `network = { mode = "gated", egress = { allow = [...] } }` | Egress through the node datapath, judged by policy | **No**, refused at submit | Any number |

Gated networking is the one with per-sandbox egress policy in Legion, and it
can't take a memory checkpoint yet because the guest's vsock datapath is part
of its saved state. A gated sandbox can still suspend with
`checkpoint = "disk"`, which keeps `/workspace` and boots fresh on resume. See
[A spec is rejected at submit](#a-spec-is-rejected-at-submit) and
[sandbox networking](../task-runtime-sandbox.md#networking).

Pick by what the sandbox must keep. If a resumed sandbox must continue its
running processes, use an auto TAP and enforce egress on the bridge. If
egress must be judged per sandbox by Legion's policy, run it gated with disk
checkpoints.

## The platform's lifecycle

Every command below takes `-j` for the standard JSON envelope, and every
mutating one takes `--idempotency-key`, so a retry after a network blip
returns the first answer with `deduplicated: true`.

### Create a user's sandbox

```console
$ legion instance create agents --id user-42 -j
{"ok":true,"data":{"instanceIds":["user-42"],"deduplicated":false}}
```

The instance is `active` once its allocation is placed. The scheduler packs it
on its 512 MiB reservation, and the VM boots with 4 GiB. A user's id is theirs
for the life of the job: after `instance destroy user-42`, a new
`create --id user-42` fails with `AlreadyExists`.

### Watch it go idle and suspend

The worker reports each sandbox's idle time every 10 seconds, and the manager
sweeps every 15. `instance get` shows the window while it runs:

```console
$ legion instance get user-42
...
idle since: 2026-10-10T12:00:00Z (suspends after 3m0s)
```

```console
$ legion instance get user-42 -j
{"ok":true,"data":{"instance":{"instance_id":"user-42","job_id":"agents",
 "mode":"keyed","state":"active","current_alloc_id":1042,
 "idle_since":"2026-10-10T12:00:00Z","idle_after_seconds":180, ...},
 "provisional":null}}
```

About `after` plus 30 seconds later, the sweep starts a suspend. The node
captures the VM, stops it, and the manager frees the reservation in one
transaction:

```console
$ legion instance get user-42 -j
{"ok":true,"data":{"instance":{"instance_id":"user-42","state":"suspended",
 "current_alloc_id":null,"checkpoint_id":"snap-91c2a0b3d4e5f601",
 "checkpoint_kind":"memory","checkpoint_durability":"local",
 "checkpoint_node_id":"w-03","checkpoint_bytes":1918893056,
 "suspended_at":"2026-10-10T12:03:24Z","suspend_reason":"idle",
 "last_suspend_ms":740, ...},"provisional":null}}
```

`suspend_reason` says who did it: `manual`, `idle`, `pressure`, or `make_room`. To follow
it as events, tail `legion events list --kind instance.suspended`.

Your platform can also suspend on its own signal, such as a closed browser
tab:

```console
$ legion instance suspend user-42 --wait -j
{"ok":true,"data":{"instance":"user-42","state":"suspended",
 "checkpoint":"snap-91c2a0b3d4e5f601","kind":"memory","bytes":1918893056,
 "duration_ms":740,"node":"w-03","durability":"local",
 "message":"...","deduplicated":false}}
```

### Resume

```console
$ legion instance start user-42 --wait -j
{"ok":true,"data":{"instance":"user-42","state":"active",
 "resumed_from":"snap-91c2a0b3d4e5f601","node":"w-03","restore_ms":212,
 "detail":null,"message":"Resuming instance user-42 from snap-91c2a0b3d4e5f601 on w-03",
 "deduplicated":false}}
```

The resume is pinned to `w-03`, because a `local` checkpoint exists only
there. It runs as a new allocation with a new id, so its workload identity is
issued fresh. The guest wakes mid-instruction, the resume handler refreshes
its hostname, clock, RNG, and identity files, and then `onResume` runs. The
target is under 300 ms for a 1 GiB guest on local NVMe. Placement time comes
on top, so a node with no room adds a wait. See
[resuming](../suspend-and-resume.md#resuming).

Without `--wait`, `start` returns at once with `"state":"pending"`. Poll
`instance get` until `state` is `active` with no `pending_op`.

### Force a cold boot

```console
$ legion instance start user-42 --cold --wait -j
{"ok":true,"data":{"instance":"user-42","state":"active","resumed_from":null,
 "node":"w-03","restore_ms":null, ...}}
```

`--cold` discards the checkpoint and boots the image fresh on the retained
`/workspace`. Running processes and anything written outside `/workspace` are
gone. It is the escape hatch when a checkpoint can't be restored, and it is
also how a user gets a clean VM without losing their files.

### Fork

An agent that wants to try three approaches forks its sandbox, works in each
copy, and keeps the winner:

```console
$ legion instance fork user-42 --count 3 --wait -j --idempotency-key fork-7f2
{"ok":true,"data":{"origin":"user-42","snapshot":"snap-c0de4a1b2c3d4e5f",
 "pause_ms":610,"instances":["user-42-f1","user-42-f2","user-42-f3"],
 "kind":"memory","node":"w-03","durability":"local",
 "checkpoint_state":"local","deduplicated":false}}
```

> **A static IP in the guest is copied to every fork.** Each fork gets its
> own MAC and TAP device, but an address configured inside the guest comes
> along, so three forks would answer on `user-42`'s IP on `br0`. Have the
> image use DHCP, and renew the lease in `onResume`, before you fork
> sandboxes with networking.

The origin pauses while its memory is written (`pause_ms`, about 0.5 to 1
second per GiB of guest memory on local NVMe), then keeps running. Each fork
is a new keyed instance with its own id, its own copy of `/workspace`, its
own MAC, RNG seed, and workload identity, and `onResume` runs in each one.
The fork ids derive from `user-42`, so they stay in your namespace under
`idSource = "caller"`. Forks run on the origin's node, because the checkpoint
and the `/workspace` copies live there. Destroy the losers with `legion
instance destroy user-42-f2`, and destroy (don't just stop) a fork you no
longer want, since a stopped fork that never restored keeps its checkpoint
on the node.

`--spread` puts each fork on its own node instead. It publishes the
checkpoint first, so it needs a worker `contentStore` and a sandbox with no
`stateful.storage` (a fork's `/workspace` copy is cloned on the origin's
node). With the spec above it is refused with that hint. See
[spreading forks](../suspend-and-resume.md#spreading-forks-across-nodes).

Forks of one memory checkpoint on Firecracker share unmodified pages through
the page cache, so three forks of a 4 GiB guest start well under 12 GiB of
real memory. Legion still charges each fork its full 512 MiB reservation,
because sharing decays as forks diverge. A fork needs `snapshot:write` on the
job as well as `job:write`, since it reads everything the origin held in
memory. See [fork](../suspend-and-resume.md#fork).

### Start from a golden snapshot

Booting the image and installing dependencies on a user's first command is
slow. Instead, build one sandbox the way every user should start (packages
installed, caches warm, language server indexed), snapshot it, and start new
users from the snapshot:

```console
$ legion instance create agents --id golden-2026-41
$ # ...let your harness install packages and warm its caches...
$ legion snapshot create golden-2026-41 --memory --name agent-golden --wait

$ legion instance create agents --id user-43 --from-snapshot agent-golden -j
{"ok":true,"data":{"instanceIds":["user-43"],
 "restoringFrom":"snap-5b1e0f9a7c3d2e18","deduplicated":false}}
```

`user-43` restores the golden VM with its own copy of the volume and its own
identity, exactly like a fork. The snapshot is untouched and seeds any number
of later creates. The weekly rebuild takes a new snapshot and moves the name:

```console
$ legion snapshot alias set agents agent-golden snap-7a0c3e5b9d1f2468
Moved agent-golden from snap-5b1e0f9a7c3d2e18 to snap-7a0c3e5b9d1f2468
```

With this spec the golden snapshot is `local`, and two limits follow. Every
instance created from it runs on the snapshot's node, so one golden snapshot
fills one node, and a create waits when that node is full. A published golden
snapshot lifts that (see [Surviving a node loss](#surviving-a-node-loss)),
but only for a task without `stateful.storage`. The snapshot must be a
manually taken snapshot of the same job and task, and one call creates at
most 64 instances. See
[create from a snapshot](../suspend-and-resume.md#create-from-a-snapshot).

### Driving it from an agent

The same lifecycle is available over MCP, so an orchestrating agent can
create, park, and resume sandboxes by name:

```bash
legion mcp serve --allow-write
```

| Step | MCP tool | Arguments |
| --- | --- | --- |
| Create | `legion_instance_create` | `job_id`, `instance_id`, `idempotency_key` |
| Check | `legion_instance_get` | `instance_id` |
| Park | `legion_instance_suspend` | `instance_id`, `checkpoint` (optional), `idempotency_key` |
| Resume | `legion_instance_start` | `instance_id`, `cold` (optional), `idempotency_key` |
| Check room | `legion_node_usage` | `node_id` |
| Fork | `legion_instance_fork` | `instance_id`, `count`, `checkpoint` (optional), `spread` (optional), `idempotency_key` |

`legion_instance_suspend` and `legion_instance_start` return at once. Poll
`legion_instance_get` until `state` is `suspended` (or `active`) and
`pending_op` is clear. A suspend that fails leaves `state` as `active`, with
the reason in `detail`. Always pass an `idempotency_key`, so a retried call
after a timeout can't suspend twice or create two sandboxes.

Give the agent a token scoped to the `agents` job. A memory suspend needs
`snapshot:write` on top of `job:write`, so decide deliberately whether an
agent should hold it (see [Security](#security)). See
[agent readiness](../agent-readiness.md#1-mcp-server-legion-mcp-serve).

## Capacity

### Reading `legion node usage`

```console
$ legion node usage w-03
Node w-03 (ready)  usage sampled 4s ago

 RESOURCE  CAPACITY          RESERVED           USED             PRESSURE
 memory    96256 MiB ×1.25   92160 MiB (77%)    51200 MiB (43%)  ok (free 41000 MiB, stall 0.2%)
 cpu       64000 MHz         45000 MHz (70%)    21000 MHz (33%)

 ALLOCATION  JOB     INSTANCE  RES/MAX MiB  USED MiB  PEAK MiB  CPU   IDLE
 1042        agents  user-42   512/4096     3100      3900      1.40  -
 1077        agents  user-17   512/4096     610       1200      0.01  2m41s
 1093        agents  user-08   512/4096     505       540       0.00  41s
 ...
```

- **CAPACITY** is what the scheduler packs against: physical memory minus
  `reservedResources`, times the ratio, shown after the `×`.
- **RESERVED** counts only running sandboxes. A suspended sandbox holds
  nothing, so it isn't here.
- **USED** is measured. `USED` well under `RESERVED` means the ratio has room
  to rise. `USED` near physical memory means it doesn't.
- **RES/MAX** against **USED** shows which sandboxes are bursting. `user-42`
  is running a build at 3100 MiB of a 512 MiB reservation, so it is the first
  to give memory back.
- **IDLE** shows how long each sandbox has been quiet, by its task's idle
  policy. A row that never shows an idle time is a sandbox that will never
  suspend on its own.

`-j` prints the `GetNodeUsage` reply. Each allocation carries
`reserved_memory_mib`, `memory_max_mib`, `usage`, `idle_seconds`, `band`,
`pressure_rank`, and `pressure_steps`, and the node's `memory` block carries
`oversubscribe_ratio` and `pressure_response`. See
[measuring usage](../resource-enforcement.md#measuring-usage).

### Under pressure, in order

`node usage` also prints the order the node would act in if it ran short now:

```console
UNDER PRESSURE, IN ORDER
 #  ALLOCATION     BAND    RES/USED MiB  STEP
 1  1042 agents    normal  512/3100      reclaim, then suspend
 2  1077 agents    normal  512/610       reclaim, then suspend
```

Here is what each step does to a sandbox:

- **`reclaim`** inflates the guest's balloon by what it uses above its 512
  MiB reservation, and never further. The guest gives that memory back to the
  host. It can slow a guest down and never kills it, because the balloon
  deflates on the guest's own OOM. The balloon deflates again when the
  pressure clears. Nothing stops, and the user may not notice.
- **`suspend`** checkpoints and stops sandboxes with `onPressure = true`,
  quietest first, in any band. The node acts on its own and reports after,
  because waiting for the manager is wrong when memory is running out. The
  user's sandbox is `suspended` with `suspend_reason = "pressure"`, and the
  next `start` resumes it like any other suspend.
- **`evict-bursting`** and **`evict-reserved`** never touch these sandboxes.
  Their storage lives only on this node, so an eviction would strand them.
  They act on other governed work on the node.

Within a step, lower bands go first. That is why the spec puts sandboxes in
`normal`: a `low` batch job sharing the node gives way before them. See
[see the order before it matters](../resource-enforcement.md#see-the-order-before-it-matters).

### Disk for parked sandboxes

A suspended sandbox frees its memory and moves it to disk. Each memory
checkpoint is pinned in the node's content store until the sandbox resumes,
and it counts toward `content.budgets.snapshots`. The `bytes` in the suspend
reply, and the `legion_checkpoint_bytes` histogram, tell you what one costs
for your image. Size the budget as parked sandboxes per node times that, plus
your snapshots. A node that parks 300 sandboxes at 1.5 GiB each needs about
450 GiB.

### Room to resume

A resume is pinned to its checkpoint's node and needs its 512 MiB
reservation free there. When the node is full, the resume waits
`resumeTimeout`, and then `onRestoreFailure = "cold"` gives up the checkpoint.
With `local-dir` storage the cold boot is pinned to the same node too, so the
user loses their running state and still waits. Leave headroom on sandbox
nodes for the resumes you expect at your daily peak, and don't let creates
fill a node to the last reservation.

## Surviving a node loss

A `local` checkpoint dies with its node. `durability = "store"` publishes
each checkpoint to a shared content store after it is captured, so a resume
can run on any compatible node:

```nickel
# worker config: each region's workers point at their own bucket
contentStore = { url = "s3://legion-content-usw2/" },
```

```nickel
suspend = {
  checkpoint = "memory",
  durability = "store",
},
```

The sandbox is `suspended`, and its memory freed, as soon as the checkpoint is
on local disk. The upload happens after, and the snapshot moves from `local`
to `published` when it lands. From then on a resume goes to any node whose
VMM and CPU match the checkpoint's `compat` block, the
[`locality` score](../artifacts.md#content-locality) prefers a node that
already holds the parts, and the chosen node fetches what it lacks before it
restores.

The volume has to move too. A checkpoint is half the sandbox, and Legion does
not copy node-local volumes. So `durability = "store"` is refused at submit
for a task whose `stateful.storage` uses `local-dir`. Sandboxes attach block
devices, and no built-in network driver serves block mode, so a sandbox that
resumes anywhere today keeps everything in its root disk and memory and drops
`stateful.storage`. The checkpoint carries both. Push durable work to your
own storage (git, object storage) from inside the guest.

A published golden snapshot seeds instances on any compatible node, so it
can start a whole fleet rather than one node. Publish it from a job whose
task has no `stateful.storage` (here `agents-portable`, since `--publish`
refuses a snapshot of the `local-dir` spec above), and warm the nodes ahead
of time:

```console
$ legion snapshot create portable-golden-41 --memory --name agent-golden --publish --wait
$ legion snapshot warm agent-golden --job agents-portable --constraint region=us-west
Warming snapshot snap-5b10e2c7a9d04f31 on 2 node(s) (1 already warm)
Fetching on: w-05, w-07
Already warm: w-03
```

A publish that fails is retried by the manager twice, after one and then five
minutes, and the snapshot keeps working from its node meanwhile.

Warming is best effort. It never blocks placement, and warmed parts are
ordinary cache entries that LRU can evict. See
[durability](../suspend-and-resume.md#durability-surviving-the-node).

## Security

**`snapshot:write` is read access to memory.** A memory checkpoint holds
everything the guest had in memory: API keys, the user's tokens, decrypted
secrets. So a memory suspend needs `snapshot:write` as well as `job:write`,
and so do `snapshot create --memory` and fork. Grant `snapshot:write` on the
`agents` job as carefully as you would read access to every secret your
users' sandboxes have held. A disk suspend needs only `job:write`, and
resume needs only `job:write`. See
[auth and ACLs](../auth-and-acls.md#resources-and-actions).

**Every restore gets a fresh identity.** A resume runs as a new allocation,
so the worker issues that allocation's workload identity tokens before the
restore, writes them to the guest's identity disk, and the resume handler
publishes them under `/legion/identity/` and in `/legion/resume.env` before
`onResume` runs. The handler also reseeds the RNG, steps the clock, and sets
the hostname, so two restores of one checkpoint never share a random seed.
Processes already running keep the environment they started with, so a
long-lived process re-reads the token from `/legion/identity/` or is restarted
by `onResume`. See
[identity after a resume](../suspend-and-resume.md#identity-after-a-resume).

**A captured token stays valid until it expires.** Tokens can't be revoked.
A checkpoint taken less than one TTL ago holds a token that still works, for
anyone who can read the checkpoint. Keep identity TTLs short on sandboxes.

**Tokens refresh without a reboot.** The spec delivers its `platform` token
as a file, `/legion/identity/platform`. At about half the TTL the worker
re-issues it and rewrites the running guest's identity disk in place, and the
guest's handler rewrites the file. Nothing restarts, and the sandbox keeps
its running state, so a short TTL costs nothing. Have the harness read the
file on each call rather than once at startup. An env-delivered identity
works the same way on this sandbox: the env keeps the boot token, and
`/legion/identity/<ENV>` the current one. `changeMode = "restart"` would
reboot the guest on every refresh, so `job run` warns if you set it. See
[workload identity on sandboxes](../workload-identity.md#sandboxes).

**A restore never widens what the sandbox may do.** It always runs under the
task's current config (network policy, mounts, identity), never values from
the checkpoint. The checkpoint's `compat` block is the only thing placement
reads from it.

**Checkpoints stay on the node.** Memory parts are files owned by the worker
in its state directory. With `durability = "local"`, nothing is uploaded.

## When things go wrong

### A resume cannot restore

**After a VMM upgrade.** A memory checkpoint resumes only on the VMM version,
CPU vendor, model, and features it was taken on. Upgrade Firecracker on
`w-03`, and every sandbox suspended there before the upgrade can't restore.
The placement error names the attribute and both values:

```text
Failed to place task workspace: instance user-42 restores memory snapshot
snap-3f9a0c21d4e5b6a7, which node w-03 cannot run: sandbox.vmm.firecracker.version
is "1.12.0", the checkpoint needs "1.7.0". A memory snapshot resumes only on the
CPU and VMM it was taken on; restore an older snapshot, or restart the instance cold
```

With `onRestoreFailure = "cold"`, the resume waits `resumeTimeout` and then
boots fresh on `/workspace`, with `instance.restore_failed` (`outcome: cold`)
and the reason in `detail`. Plan VMM upgrades like a drain: resume or cold-boot
the node's suspended sandboxes before you upgrade, or accept that they come
back cold. The node attributes are in
[where a memory checkpoint can be restored](../task-runtime-sandbox.md#where-a-memory-checkpoint-can-be-restored).

**After a node goes down.** A down node holds the checkpoint hostage. The
resume waits `resumeTimeout` for it, and then `cold` gives up the checkpoint:

```text
detail: the resume could not be placed on checkpoint snap-91c2a0b3d4e5f601's node
within resumeTimeout (30s); booted cold on the retained storage
```

With `local-dir` storage the volume is on that node too, so the cold boot
still waits for the node to come back. It just no longer waits for the
checkpoint. When you remove the node from the cluster for good, its
checkpoints become `lost`, and the next `start` boots cold at once:

```text
detail: checkpoint snap-91c2a0b3d4e5f601 is lost (its node was removed from the
cluster); booted cold on the retained storage
```

Under `onRestoreFailure = "fail"`, `start` on a lost checkpoint refuses and
the instance stays suspended:

```text
Error: instance user-42 cannot resume: checkpoint snap-91c2a0b3d4e5f601 is lost (its node
was removed from the cluster); the instance stays suspended (onRestoreFailure = "fail")
Hint: `legion instance start user-42 --cold` boots it fresh on its retained storage
```

`legion instance start --wait` on a resume that fails to restore exits
non-zero with `RestoreFailed` and the same `--cold` hint. To find the users a
dead node took with it, list instances whose checkpoint lived there:
`legion instance list agents -j` and filter on `checkpoint_node_id`.

### A suspend fails

**A suspend never stops a sandbox it failed to capture.** The guest is
resumed, the instance stays `active`, and `instance.suspend_failed` carries
the node's error:

```text
Error: instance user-42 was not suspended and keeps running: <the node's error>
Hint: see `legion events list --kind instance.suspend_failed`
```

The usual causes:

- **The snapshot budget is full.** Parked checkpoints are pinned, so a node
  that parks more than `content.budgets.snapshots` holds refuses new ones.
  Raise the budget or move users off the node.
- **`was not launched for memory checkpoints`.** The sandbox started before
  the `suspend` block was added. Restart it, or suspend with
  `--checkpoint disk`.
- **`content.enabled = false`** on the worker. Memory checkpoints need the
  content store.

An idle suspend that fails is tried again about a minute later, while the
sandbox stays idle. A pressure suspend that fails moves the node on to its
next candidate.

### An instance never goes idle, or goes idle too soon

An instance stays awake while any listed signal fires. What each one sees on
a microVM matters:

| Signal | What it sees | On an auto-TAP microVM |
| --- | --- | --- |
| `network` | Bytes through the node datapath, plus bytes the guest sends on its own NIC | Bytes the guest sends on its TAP (`rx_bytes` of `/sys/class/net/lgf...`) |
| `connections` | Open ingress connections through the datapath | Refused at submit. No datapath fronts the guest |
| `cpu` | CPU at or above `cpuBelow` of the task's `cpu` | The VMM's CPU, which is the guest's |
| `exec` | Open `exec` sessions and actions | Never fires. A microVM takes no exec sessions, so `job run` warns when it is listed |

Only bytes the guest sends count. What arrives on `br0` includes broadcast
noise that would otherwise keep every sandbox awake. Any real exchange has
the guest sending something, even if it is only acknowledgements. Three
failure modes follow:

- **It never suspends.** Something in the guest keeps the CPU above
  `cpuBelow` (0.02 of `cpu`, never below 1% of a core), such as a file
  watcher, a language server reindexing, or a spinning harness. Or it keeps
  sending: a heartbeat to your platform, an NTP client, frequent DHCP
  renewals. Compare `instance get`'s idle window and `node usage`'s `CPU` and
  `IDLE` columns with what the guest runs, then quiet the process, raise
  `cpuBelow`, or drop `network` from `signals`.
- **It suspends in the middle of a model call.** An agent harness waiting
  40 seconds on a model holds a mostly quiet HTTPS connection, sends almost
  nothing, and uses almost no CPU, so the wait counts as quiet. No signal sees
  an idle open connection from the guest, and `connections` is refused for
  that reason rather than accepted and ignored. Set `after` longer than your
  longest model call, or leave `idle` off and suspend from your platform,
  which knows when a call is in flight.
- **It suspends while someone is in a shell.** Not on a microVM: `legion
  exec` doesn't reach the guest. A shell your platform runs over the network
  is traffic on the TAP, which keeps it awake while it sends.

The `hint` signal, where the workload says it is parked, is not available,
and a spec that lists it is refused:

```text
Error: jobspec failed validation: task "workspace": suspend.idle.signals "hint" is not
implemented yet: the workload's own busy/idle signal ships in suspend plan Phase 8
(remove "hint". To keep an instance awake through a quiet call, add "connections"
(any open ingress connection through the node datapath counts as activity; a sandbox
sees it only on gated networking)) [code=InvalidArgument]
```

So does `connections` on this sandbox:

```text
Error: jobspec failed validation: task "workspace": suspend.idle.signals "connections" counts
open ingress connections through the node datapath, and this sandbox's traffic never crosses it
(it is not network.mode = "gated", so clients reach the guest directly or not at all)
(remove "connections". To keep a sandbox awake through a quiet model call, set idle.after
longer than the call, or suspend from your platform, which knows when a call is in flight)
[code=InvalidArgument]
```

**Nothing ever suspends on a node.** Read `detail` in `instance get`. A node
without `resourceEnforcement.enabled` sends no usage reports, and the policy
says so:

```text
detail: idle-unmeasured: the task's suspend.idle policy (after 180s) cannot act
because node w-03 sends no usage reports (set resourceEnforcement.enabled on its
worker type), so the instance will not suspend when idle
```

### A spec is rejected at submit

Submit, `job validate`, and `job plan` refuse a spec that asks for memory
checkpoints on a gated network:

```text
$ legion job validate agents-gated.ncl
Error: jobspec failed validation: task "workspace" asks for memory checkpoints, but
sandbox memory checkpoints don't support gated networking yet (set snapshots.memory
= false for disk-only snapshots, or drop network gating for this task) [code=InvalidArgument]
```

The spec asks twice, once in `snapshots` and once in `suspend`, and the hint
names the first one it finds. Fixing that one surfaces the other:

```text
Error: jobspec failed validation: task "workspace" asks for memory checkpoints, but
sandbox memory checkpoints don't support gated networking yet (set suspend.checkpoint
= "disk" to suspend with a disk checkpoint, or drop network gating for this task)
[code=InvalidArgument]
```

A gated sandbox with `snapshots.memory = false`, `suspend.checkpoint = "disk"`,
and no `onResume` validates. It suspends by stopping and resumes by booting
fresh on `/workspace`. When gating comes from a stored network policy rather
than the spec, the sandbox launches without checkpoint support, and a memory
suspend or snapshot fails with `FailedPrecondition` and the same hint.

## The macOS dev loop

A Mac runs sandboxes on Apple `container` (dev mode), and with
`resourceEnforcement.enabled` its worker measures them through `container
stats`: CPU, memory, and the bytes each container sends, plus `legion alloc
exec` sessions. So the idle policy works on a Mac. It can't take memory
checkpoints, so keep a dev copy of the spec that suspends to disk and boots
cold on resume:

```nickel
# agents-dev.ncl
{
  name = "agents-dev",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    stateful = {
      mode = "keyed",
      idSource = "caller",
      # Dev mode bind-mounts volumes, so no `mode = "block"`.
      storage = { path = "/workspace" },
    },
    config = {
      image = "ghcr.io/acme/agent-base:2026-10",
      command = ["/usr/local/bin/harness"],
    },
    resources = { cpu = 250, memory = 512 },
    suspend = {
      checkpoint = "disk",
      # Short, so you can watch it happen. `exec` works here: dev mode
      # takes `legion alloc exec` sessions.
      idle = { after = "1m", signals = ["network", "cpu", "exec"] },
    },
  }],
}
```

Give the Mac's worker type the measuring switch, in the manager's
`workerConfigPath`:

```nickel
# workers/devsandbox.ncl
{ workerType = "devsandbox", resourceEnforcement = { enabled = true } }
```

Then:

```bash
container system start                  # if the Apple container service is down
legion join -a --type devsandbox
legion job run agents-dev.ncl
legion instance create agents-dev --id user-42
legion instance get user-42             # "idle since ..." once the first report lands
# about a minute and a half later: suspended, suspend_reason idle
legion instance start user-42 --wait    # a cold boot on the same /workspace
```

If a long-running local cluster already holds `:4747` and `:4848`, run the
dev cluster on its own ports: set `server.port` and `storePath` in a copy of
`configs/manager.ncl`, and `LEGION_WORKER_PORT` and `LEGION_WORKER_STATE_PATH`
for the worker. Each dev-mode container is named after its node and
allocation, so two local clusters never remove each other's containers.

What the Mac doesn't do: memory checkpoints, forks of running state, auto
TAPs, the pressure response, and refreshing an env-delivered token (file
tokens refresh through a read-only bind mount at `/legion/identity`). See
[the macOS dev loop](../suspend-and-resume.md#macos-dev-loop).

## Limitations

- **Memory checkpoints need Linux, KVM, and Firecracker or cloud-hypervisor.**
  On macOS (Apple `container`) and Linux dev mode, a sandbox suspends to disk
  only. See [The macOS dev loop](#the-macos-dev-loop).
- **Linux verification is pending.** These features were built and tested
  on macOS, with the Linux-only parts (cgroups, the VMM checkpoint and restore
  calls, the balloon, the guest's resume handler, auto TAPs and their
  counters) unit-tested against fixtures. Some of that code has not yet run against a real kernel and VMM,
  so expect rough edges on your first Linux hosts.
- **Gated networking and memory checkpoints don't fit together yet.** An
  auto TAP does, with egress enforced on your bridge rather than by Legion's
  policy. See [Networking](#networking).
- **Idle detection on a microVM sees CPU and the bytes the guest sends**,
  never an idle open connection, for the reasons in
  [An instance never goes idle](#an-instance-never-goes-idle-or-goes-idle-too-soon).
  Activity is sampled every 10 seconds and swept every 15, so `after` is
  honored to within about 30 seconds.
- **A checkpoint restores only on the same VMM version, CPU, and guest
  shape.** A VMM upgrade turns the node's parked sandboxes into cold boots.
- **Local checkpoints die with their node**, and `local-dir` volumes pin
  sandboxes to it. Cross-node resume needs `durability = "store"` and no
  node-local storage.
- **A refreshed token lands in a file, not in the env.** A process that read
  its token once at startup keeps the old one. See [Security](#security).
- **No exec into a running microVM**, so `legion exec` and actions don't
  reach the guest.
- **A pressure suspend can be lost if the worker restarts before reporting
  it.** The node then starts the sandbox again, fresh, on its retained
  storage.
- **The worker's task lock is held while the VMM writes guest memory.** For a
  guest of several GiB on slow disk, a suspend can delay that node's
  heartbeats.
- **The resume handler is the worker's own binary**, copied into the guest. A
  guest whose libc can't run it (a musl image with a glibc worker build)
  restores without refreshing its identity, and init says so on the console.
- **Fork and create-from-snapshot run on the snapshot's node** unless the
  checkpoint is published, which needs a task without `stateful.storage`.
- **A static IP in the guest is copied to every fork.** Use DHCP, or renew
  the address in `onResume`.

The full list is in the
[suspend and resume limitations](../suspend-and-resume.md#limitations),
[sandbox limitations](../task-runtime-sandbox.md#current-scope-and-limitations),
and [the limits of the pressure response](../resource-enforcement.md#limits-of-the-response).

## See also

- [Suspend and resume](../suspend-and-resume.md): the suspended state,
  snapshots, idle and pressure suspend, events, metrics
- [Resource enforcement](../resource-enforcement.md): `memoryMax`,
  oversubscription, the pressure steps
- [The sandbox runtime](../task-runtime-sandbox.md): microVMs, guest size,
  checkpoints
- [Stateful workloads](../stateful-workloads.md): keyed instances
- [Running Postgres](postgres.md): the same stateful machinery for a database
