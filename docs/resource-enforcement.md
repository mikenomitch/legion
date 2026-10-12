# Resource Enforcement

On Linux, the kernel can enforce the `resources` a task declares, instead of
the scheduler only counting them. This page covers what each field means when
enforcement is on, how the worker divides up the node, how a task bursts past
its reservation, how a node is packed past its physical memory, what the node
does when it runs short, and how all of it fails.

Enforcement is **on by default**. Memory pressure, oversubscription,
`memoryMax` bursting, usage reports, and sandbox confinement all depend on
it, so a worker gets them with no configuration. The cost is that
`resources.memory` is a real limit: a task that declares less memory than it
uses is OOM-killed instead of quietly running over.

## Defaults and turning it off

A worker config needs no `resourceEnforcement` block. These are the
defaults:

```nickel
# configs/workers/default.ncl
{
    resourceEnforcement = {
        enabled = true,
        # Hierarchy root under /sys/fs/cgroup. Legion creates it, then
        # `share/` and `reserve/` beneath it, and one cgroup per allocation
        # beneath those.
        cgroupParent = "legion",
    },
}
```

Set `enabled = false` to treat `resources` as scheduling estimates only. Do
that for a pool whose jobs declare rough numbers you don't want enforced yet.

Requirements:

- the **cgroup v2 unified hierarchy** (`/sys/fs/cgroup/cgroup.controllers`
  exists),
- a worker that can create and write the `cgroupParent` subtree at the cgroup
  root, which is the standard setup when the worker runs as root. A
  configurable root for delegated non-root setups is not implemented.
- the `cpuset`, `cpu`, and `memory` controllers available for delegation.

If a worker can't enforce (no cgroup v2, no write access to create
`cgroupParent`, or no usable CPU fingerprint), it **logs a warning at startup
and runs tasks unconfined** instead of failing every allocation. Enforcement
shouldn't be the reason a workload won't run. A non-root worker and a worker
in a container with a read-only `/sys/fs/cgroup` both land here. On macOS
the switch measures sandboxes for usage reports and idle policies and
confines nothing.

A Linux worker that does enforce logs, once at startup, `Resource enforcement
enabled: governed tasks run in worker-owned cgroups` with the cgroup root and
the usable CPU list. That line is logged by the governor itself after it has
checked the node, so its absence (with one of the warnings above in its
place) means tasks run unconfined. Once a governed task is running,
`/sys/fs/cgroup/<cgroupParent>/share/` (or `reserve/`) holds a
`legion-<allocation>` leaf with the task's `memory.max`.

## What each field means

| Jobspec | Kernel meaning (Linux, enforcement on) |
| --- | --- |
| `resources.memory` (MiB) | The **reservation**: what the scheduler packs on, and the cgroup's `memory.low`, so the kernel reclaims from tasks above their reservation before it touches this one. Without `memoryMax` it is also the **hard** `memory.max`, with no swap allowance on top, so a leaking task can't get around the limit by swapping. Going over it triggers a kernel OOM kill of *that task's* cgroup. (The container runtime writes this as OCI `memory.swap == memory.limit`, and a worker-created cgroup gets `memory.swap.max = 0`. Cgroup v2 counts swap on its own while the OCI spec counts memory plus swap, so both express the same rule.) |
| `resources.memoryMax` (MiB) | The **ceiling** a task may burst to when the node has room: the hard `memory.max`, in place of `memory`. Never scheduled on. See [Burstable memory](#burstable-memory-memorymax). |
| `resources.cores` (whole CPUs) | **Exclusive pinned CPUs.** The worker picks specific logical CPUs and removes them from every other governed task's CPU set. |
| `resources.cpu` (MHz) | A **proportional weight** (`cpu.weight`, using the standard `shares → weight` mapping with shares == MHz). It only matters under contention. On an idle node every task can burst. On a saturated node each task slows down in proportion to what it declared. |

Two rules follow, checked at submit on every governed runtime:

- **`cores` must be a whole number.** A cpuset can't hold half a CPU. Before
  enforcement existed, fractional values were silently rounded on macOS and
  ignored on Linux. A spec with `cores = 1.5` that used to be accepted now
  fails, with a message that names both fixes.
- **Declare `cores` or `cpu`, never both.** They're two different CPU
  contracts: exclusive whole CPUs versus a share of the common pool. There's
  no sensible way to combine them.

**Only declared values are enforced.** A job that declares nothing gets no
memory or CPU limit. Scheduler defaults aren't folded in. The task still
lands in the hierarchy, which keeps it off other tasks' reserved cores.

`resources.cpus` is an older alias for `cpu`, so it is in MHz too, not a CPU
count: `cpus = 0.5` reserves half a megahertz, and `cpus = 1` reserves one.
Declare `cpu = 500` for a share of about half a 1 GHz core, and `cores` for
whole exclusive CPUs. Submit warns about any `cpu` or `cpus` under 10, since
that is almost always a CPU count in the wrong unit.

## Burstable memory: `memoryMax`

A service that idles at 300 MiB and spikes to 2 GiB used to have two bad
choices: reserve 2 GiB forever, or reserve 300 MiB and get OOM-killed on the
spike. `memoryMax` splits the two numbers, the way Nomad's `memory_max` does:

```nickel
resources = {
    cpu = 500,          # MHz
    memory = 400,       # reserved: what the scheduler packs on
    memoryMax = 2048,   # ceiling: hard limit, enforced where the runtime can
},
```

- **The scheduler packs on `memory` only.** `memoryMax` is not a resource a
  node has to have free, so a task whose ceiling is larger than any node
  still places on its reservation.
- **The cgroup gets `memory.max = memoryMax` and `memory.low = memory`.**
  `memory.low` is what makes bursting safe for everyone else: under memory
  pressure the kernel reclaims from cgroups above their `memory.low` before
  it touches any cgroup within it. A task with no `memoryMax` gets
  `memory.low = memory.max = memory`, so its whole reservation is protected.
- **Unset means today's behavior.** The reservation is the limit.
- **`memoryMax` must be at least `memory`.** Submit rejects `memoryMax`
  below the reservation, including a `memory` that comes from the manager's
  `taskResourceDefaults`:

  ```console
  Error: resources.memoryMax (256 MiB) is below resources.memory (512 MiB). `memory` is
  what the scheduler reserves and `memoryMax` is the ceiling the task may burst to, so the
  ceiling must be at least the reservation. Raise memoryMax, or lower memory
  ```

- **On a runtime that doesn't enforce resources** (`noop`, third-party
  plugins that haven't opted in), submit accepts `memoryMax` and warns that
  nothing caps the task at it.
- **The `sandbox` runtime sizes its guest from it.** A sandbox with
  `memory = 512, memoryMax = 4096` boots a 4 GiB guest and reserves 512 MiB.
  See [Sandbox tasks](#sandbox-tasks).
- **On macOS**, Apple `container` sizes the guest VM from `memoryMax` when it
  is set, else from `memory`.

The protection `memory.low` gives is hierarchical: a leaf is protected only
as far as its ancestors are. The worker writes `memory.low = max` on the
cgroups it owns (`legion/`, `share/`, `reserve/`). Above `cgroupParent`, the
operator's cgroups decide. systemd mounts cgroup v2 with
`memory_recursiveprot` by default, which passes protection down, so on most
distributions there is nothing to do.

**The honest guarantee.** With `memoryMax` and no oversubscription, the sum
of reservations fits in physical memory. A task within its reservation is
then never stopped for node memory pressure. Only tasks above their
reservation compete for what is left, and they are the ones the [memory
pressure response](#memory-pressure) acts on.

## Oversubscription

When nodes are reserved at 90% but use 30%, the scheduler is leaving memory
on the table. A worker type can declare that its schedulable capacity is a
multiple of its physical capacity:

```nickel
# configs/workers/default.ncl
resourceEnforcement = {
    enabled = true,
    # Schedulable memory = (physical - reservedResources.memory) x 1.5.
    # CPU is weight-shared, so `cpu` only changes how much the scheduler is
    # willing to place and never throttles anything. Default 1.0 (off).
    oversubscribe = { memory = 1.5, cpu = 1.0 },
},
```

- **The ratio lives inside `resourceEnforcement`** because oversubscription
  without cgroups to act on is a bet on the kernel OOM killer. A ratio above
  1.0 with `enabled = false` is rejected when the manager loads the worker
  config, and a ratio below 1.0 is rejected too (use `reservedResources` to
  hold capacity back).
- **It rides registration.** The worker reports its ratio with its
  registration, and the scheduler multiplies the node's memory (and CPU MHz)
  capacity by it, after `reservedResources`. Fit checks, the read model, and
  the plan-apply recheck all see the multiplied capacity. Changing the ratio
  takes a worker restart to re-register.
- **A node that cannot enforce registers at physical capacity.** A worker
  without cgroup v2 ignores its ratio and logs a warning, so a misconfigured
  node is never packed past what it can govern.
- **`cores` is never oversubscribed.** A pinned core is exclusive.
- `legion node usage` shows the ratio on the capacity column, for example
  `65536 MiB ×1.5`, and percentages against the multiplied capacity.

**A ratio above 1.0 voids the guarantee above.** Reservations can now exceed
physical memory, so the node may have to stop a task that is within its
reservation. That is the `evict-reserved` step, and it only ever runs on an
oversubscribed node. Use oversubscription where the workloads are
idle-heavy, and keep latency-sensitive work in a pool without it.

## Suspended instances

A suspended instance gives its reservation back by default, which is the
density win of suspend and the reason its resume can find the node full. A
job keeps a share of it charged with `suspend.reserve` (see
[Capacity for resumes](suspend-and-resume.md#capacity-for-resumes)). The held
share counts in `RESERVED`, and `legion node usage` adds a `suspended:` line
under the resource table that says how much of it is held for suspended
instances.

The manager also suspends idle suspendable instances to make room for a
resume waiting on a full node (manager `suspend.makeRoom`, on by default).
See [Capacity for resumes](suspend-and-resume.md#capacity-for-resumes) for
the whole model and how to pick a value. `0.25` suits interactive fleets.

## How the node is partitioned

The worker splits its usable CPUs between two parent cgroups that it creates
and owns. This follows Nomad's model.

```
legion/
├── share/     cpuset.cpus = usable − reserved     ← unpinned tasks live here
│   └── legion-<alloc>   cpu.weight from declared MHz
└── reserve/   cpuset.cpus = union of reservations
    └── legion-<alloc>   cpuset.cpus = its assigned cores, exclusively
```

A task that declares `cores` gets whole CPUs moved out of the share slice.
Cgroup v2 cpusets are hierarchical, so **one write to
`legion/share/cpuset.cpus` updates the fence for every task at once**. There
are no per-task updates, and no window where a reserved core is still shared.

Usable CPUs come from the worker's own `cpuset.cpus.effective`, not `nproc`.
A worker that is itself confined shouldn't hand out CPUs it can't use. Cores
are assigned lowest-numbered free core first and released on stop or destroy.
They're also **saved in the worker's allocation records**, so after a worker
restart it rebuilds the allocator and both slice masks instead of giving a
running task's cores to another task.

The cgroup path is predictable: `legion/share/legion-<alloc>` or
`legion/reserve/legion-<alloc>`. You can find a task's cgroup by allocation ID
and see the share/reserve split in the hierarchy.

### Exclusivity scope

The fence covers every runtime that opts in, which today means `container`,
`exec`, and `sandbox`. Processes **outside Legion** (node daemons, SSH sessions) and tasks
on runtimes that haven't opted in aren't covered and can still run on reserved
cores. That's unavoidable on a shared host short of partitioning the whole
machine.

Legion refuses to reserve *every* core. Every unpinned governed task on the
node lives in the share slice, so at least one core has to stay there.

## The runtime interface

Enforcement lives in the worker core. Task runtimes use it through one trait
method with a default implementation, plus a struct on the instance. The
worker core handles the hard parts (CPU fingerprinting, the allocator, both
slice masks, persistence, restart recovery, and release), so a runtime that
adopts it never deals with accounting.

```rust
pub enum ResourceEnforcement {
    /// Not governed (the default). No assignment is computed and no cgroup
    /// file is touched. Honest for runtimes with nothing to confine, and for
    /// plugins compiled before this method existed — they behave exactly as
    /// they do today.
    Unsupported,
    /// Worker core creates the cgroup and writes its limits; the runtime
    /// spawns its process into it.
    WorkerCgroup,
    /// The runtime applies the assignment its own way.
    RuntimeApplied,
}
```

| Runtime | Mode | How |
| --- | --- | --- |
| `container` | `RuntimeApplied` | The assignment goes into the generated OCI spec (`cgroupsPath`, `linux.resources.memory`, `cpu.cpus` / `cpu.shares`), and the OCI runtime creates the cgroup at that path. The path is inside the worker's hierarchy, so the share mask applies to it. |
| `exec` | `WorkerCgroup` | The worker core creates and sizes the cgroup. The plugin joins it in its `pre_exec` sequence, before switching to the task user. |
| `sandbox` | `WorkerCgroup` | The worker core creates and sizes the cgroup, and the VMM process (cloud-hypervisor or Firecracker) joins it before it execs. Guest RAM is VMM memory to the host kernel, so it is charged, limited, and measured there. See [Sandbox tasks](#sandbox-tasks) for how the memory ceiling is sized. |
| `noop`, dylib plugins | `Unsupported` | `resources` are used only for scheduling. |

`container`, `exec`, and `sandbox` share one hierarchy, so a core reserved by
any of them is exclusive against **all three**. That's what makes cores
exclusive across the node and not only within one runtime.

### Sandbox tasks

A microVM's cgroup holds the whole VMM process: the guest's RAM plus the
monitor's own memory (device emulation, page tables, its heap). Limiting it to
the guest size would OOM-kill the VMM, and the guest with it, as soon as the
guest used all of its memory. So the ceiling is the guest size plus a fixed
overhead:

```
guest     = resources.memoryMax, else resources.memory
memory.max = guest + sandbox.vmmOverheadMiB (default 64)
memory.low = resources.memory
```

The overhead is a worker setting, since it depends on the VMM build and not on
the job:

```nickel
# configs/workers/default.ncl
{
    resourceEnforcement = { enabled = true },
    sandbox = {
        # MiB a sandbox cgroup allows on top of guest memory, for the VMM.
        vmmOverheadMiB = 64,
    },
}
```

Things to know:

- **The guest size follows `resources`.** A sandbox boots a guest of
  `memoryMax` MiB when set, else `memory` MiB (512 when undeclared, the
  runtime's own default, ahead of `taskResourceDefaults`), so what the
  scheduler reserves and what the VM gets can no longer drift apart. An explicit
  `config.memory_mib` still wins in this release, and submit warns when it
  disagrees with `resources`. A later release rejects the mismatch. See
  [task-runtime-sandbox.md](task-runtime-sandbox.md#guest-size).
- **`cpu` and `cores` apply to the VMM's threads**, so a guest with more
  `vcpus` than its `cores` reservation shares those cores among its vCPUs.
- **Dev-mode backends** (`hypervisor = "dev"`, the OCI runtime fallback on a
  Linux node without KVM) run the workload directly with no VMM. Their cgroup
  keeps the declared `resources.memory` as its ceiling, and the OCI runtime
  places the container in the worker's cgroup.
- **Submit rules tightened.** Sandbox tasks now get the governed-runtime
  checks above: a fractional `cores`, or `cores` together with `cpu`, fails at
  submit where it used to be accepted and ignored. For a fractional share,
  declare `cpu` in MHz instead (for example `cpu = 500`).
- `sandbox.vmmOverheadMiB = 0` is rejected when the manager loads the worker
  config. A VMM always needs some memory of its own.

For a runtime whose task is a host process, adopting enforcement takes two
lines:

```rust
fn resource_enforcement(&self) -> ResourceEnforcement {
    ResourceEnforcement::WorkerCgroup
}

// …and at spawn:
join_assignment_cgroup(&mut cmd, instance)?;   // no-op when nothing is governed
```

`join_assignment_cgroup` writes the PID into the target cgroup from a
`pre_exec` hook, so the process is confined before it execs. Some runtimes
need to join at a specific point in their own privileged sequence (as `exec`
does, before dropping to the task user). They can read
`instance.resource_assignment` directly instead.

See [docs/task-runtime-plugins.md](task-runtime-plugins.md) for the plugin
author's view, including the `ResourceAssignment` fields.

## What enforcement buys beyond limits

- **Whole-tree teardown.** All of a `WorkerCgroup` task's processes live in a
  Legion-owned cgroup, so the hard-kill step of `stop` writes `cgroup.kill`.
  That kills double-forked descendants that a kill of the tracked PID would
  leave orphaned. It runs *after* the graceful stop sequence, so a task that
  shut down cleanly is never sent SIGKILL.
- **OOM attribution.** When a task exits, the worker reads the cgroup's
  `memory.events`. If the OOM counter fired, it adds a note to the failure:

  ```
  exited with status 137; task exceeded its 256 MiB memory limit and was OOM-killed
  ```

  Without this, exit code 137 alone sends you digging through dmesg.

## Measuring usage

A reservation is a guess. To check it, workers with enforcement on measure
what every governed allocation actually uses and report it to the manager.

Every 10 seconds the worker reads each allocation's cgroup and the node's own
memory, and sends the sample on the heartbeat that falls due:

| Field | Source | Meaning |
| --- | --- | --- |
| `memory_current_bytes` | `memory.current` | Everything charged to the cgroup now, page cache included. What `memory.max` is compared to. |
| `memory_peak_bytes` | `memory.peak` | Highest use since start. On kernels before 5.19 (no `memory.peak`), the highest value the worker sampled, which can miss short spikes. |
| `memory_anon_bytes` | `memory.stat` `anon` | Heap, stack, guest RAM: memory the kernel can't drop without swap. |
| `cpu_usage_usec` | `cpu.stat` `usage_usec` | Total CPU time, monotonic. |
| `cpu_millicores` | derived | Average CPU over the last interval, in thousandths of a core. |
| `psi_memory_some_avg10` | `memory.pressure` | Percent of the last 10s some task in the allocation stalled on memory. |
| `last_activity_unix` | derived | The last interval with at least 1% of a core of CPU, traffic through the node datapath, or bytes a sandbox guest sent on its own NIC. |

The node-wide part is `MemTotal` and `MemAvailable` from `/proc/meminfo`, and
`some avg10` from `/proc/pressure/memory`.

A sandbox wired to a TAP device also has the device's `rx_bytes` read (the
bytes the guest sent), since its traffic never crosses the datapath.

**On macOS** there are no cgroups. With `resourceEnforcement.enabled` the
worker confines nothing, but it measures dev-mode sandboxes on Apple
`container` with one `container stats --format json --no-stream` call per
report: `memory_current_bytes` and `cpu_usage_usec` come from it, the bytes
each container sent count as activity, and peak, anon, and pressure stall are
not reported. That is what the idle policy reads on a Mac (see
[the macOS dev loop](suspend-and-resume.md#macos-dev-loop)). The memory
pressure response does not run there.

The manager keeps the **latest report per node in memory only**. Usage never
goes into the state store: it changes every 10 seconds and is worthless after
a restart, so a manager restart or failover shows no usage until the next
report arrives.

### `legion node usage`

```console
$ legion node usage w-03
Node w-03 (ready)  usage sampled 4s ago

 RESOURCE  CAPACITY    RESERVED          USED             PRESSURE
 memory    96000 MiB   71000 MiB (74%)   41000 MiB (43%)  free 52000 MiB, stall 0.4%
 cpu       64000 MHz   41200 MHz (64%)   18900 MHz (30%)

 ALLOCATION  JOB        INSTANCE  RES MiB  USED MiB  PEAK MiB  CPU   IDLE
 1042        workspace  user-42   512      1830      1900      0.25  -
 1077        workspace  user-17   512      402       410       0.00  2m41s
```

- **Capacity** is the node's fingerprint minus its worker-type
  `reservedResources`, the same number the scheduler packs against.
- **Reserved** is the sum of every placed allocation's `resources` after
  defaults, plus what suspended instances keep held
  ([`suspend.reserve`](#suspended-instances)). Held rows
  show as `<instance> (suspended, held)`.
- **`suspended:`** (when the node has any) counts the suspended instances
  whose checkpoint is on the node and what they hold, and **`checkpoints:`**
  shows their checkpoint bytes against the node's `snapshots` content
  budget. A full budget keeps new memory-checkpoint instances off the node
  (see [the checkpoint disk budget](suspend-and-resume.md#the-checkpoint-disk-budget)).
- **Used** sums measured allocations. CPU is converted to MHz at the node's
  average clock per core.
- `-j` prints the `GetNodeUsage` reply as JSON. Agents get the same reply from
  the `legion_node_usage` MCP tool.
- A node that reports a core count but no usable clock speed (Apple Silicon)
  has no MHz capacity. Its CPU capacity reads in cores (`10 cores`), and the
  reply's `cpu.cores` and `cpu.used_millicores` carry the figures that do not
  need a clock speed.

Without a node, `legion node usage` prints one line per node from the
`ListNodeUsage` RPC (also the `legion_node_usage_list` MCP tool and the UI's
workers index). Each entry is exactly that node's `GetNodeUsage` reply, with
the per-allocation rows left out unless asked for
(`include_allocations`):

```console
$ legion node usage
 NODE  STATUS  MEMORY           RESERVED  USED  CPU        PRESSURE
 w-03  ready   65536 MiB ×1.5   91%       64%   64000 MHz  ok
 mac   ready   32768 MiB        2%        -     10 cores   -
```

An empty node list means every node, up to 500 by node id. Nodes in pools
your token cannot read are left out.

`legion instance list` and `instance get` show the current allocation's
measured memory, CPU, and idle time in `mem MiB`, `cpu`, and `idle` columns,
and `InstanceInfo.usage` carries the full sample. Prometheus gets per-node
gauges, listed in [metrics.md](metrics.md#node-usage).

In the UI, the worker page's Usage card shows the same reply: capacity with
the ratio, reserved and used bars, the pressure status, the per-allocation
table, and the pressure order. The workers index has a memory column from
one `ListNodeUsage` call, and
the job page's instance and allocation tables show used memory against
`res/max`. See [ui-architecture.md](ui-architecture.md#usage-pressure-and-snapshots).

### What usage does not do

Usage is not a scheduling input. Placement still packs on reservations
(times the node's ratio). The one thing that acts on measured usage is the
node's own [memory pressure response](#memory-pressure).

Known gaps:

- **Only governed allocations are measured.** Tasks on `noop` and on
  third-party plugins that haven't adopted enforcement show a reservation and
  no usage, and node `used` doesn't count them.
- **Usage lives on the manager that received the heartbeat.** With several
  managers, ask the one the node reports to. Another manager answers with
  `usage_reported: false`.
- **A `sandbox` task's memory is the VMM's.** Guest pages the VMM touched stay
  charged even after the guest frees them, because there is no balloon or free
  page reporting yet.
- **Idle time ignores exec sessions.** An interactive `legion exec` session
  that uses almost no CPU still reads as idle.
- The report interval is fixed at 10 seconds.

## Memory pressure

**What happens to my task when the node runs short.** A task using no more
than its `memory` reservation is left alone (on nodes with no
oversubscription, always). A task *bursting* above its reservation gives
memory back first and is the first thing stopped if that isn't enough. A
task that sets `suspend.onPressure = true` is suspended with its state intact
instead of stopped.

A stopped task is rescheduled like any other platform-initiated stop, and its
stop reason says `node_pressure`, with the numbers. A suspended one waits,
holding no memory, until `legion instance start` resumes it (see
[suspend and resume](suspend-and-resume.md#under-memory-pressure)).

Operators set nothing to get this. The response is on whenever
`resourceEnforcement.enabled` is, with defaults that scale with the node. The
kernel OOM killer stays the backstop after the last step.

### The steps

The worker checks the node every second. When a trigger fires, it takes the
first step that still has something to act on, acts in order until the
expected freed memory covers the shortfall, then waits `settle` to see whether
that was enough before it takes another.

| Step | What it does | Who it can touch | Reversible |
| --- | --- | --- | --- |
| `reclaim` | Writes the bytes above the reservation to the cgroup's `memory.reclaim`, or for a sandbox with a balloon, inflates the balloon by that much | Allocations above their reservation | Yes, nothing stops. A balloon deflates when the episode ends |
| `suspend` | Checkpoints and stops the instance, keeping its state. The node acts first and the manager records it | Instances whose task sets `suspend.onPressure = true`, most idle first | Yes, `instance start` resumes it |
| `evict-bursting` | Stops allocations above their reservation | Allocations above their reservation | Rescheduled, `cause = node_pressure` |
| `evict-reserved` | Stops allocations within their reservation. Only when `oversubscribe.memory > 1` | Anyone below `critical` | Rescheduled, `cause = node_pressure` |

Within a step, candidates go **lowest band first** (the preemption order,
`background` before `critical`), then **most memory freed first** (`suspend`:
**quiet longest first**, by the task's idle policy when it has one, else by
its last activity). An allocation whose instance storage lives only on this
node is **never evicted**, matching the preemption rule: it could only come
back here. It still gives memory back in `reclaim`, and it can be suspended,
since a suspend keeps the storage where it is. Each step acts on an
allocation at most once per episode, and an episode ends when neither
trigger fires.

`suspend` runs before either eviction step, so a node with opted-in
instances suspends them before it stops anything. It takes any band,
`critical` included: the task's author opted in. A capture that fails leaves
the task running and the step moves on to the next candidate.

### Sandboxes and the balloon

A sandbox guest's memory is anonymous memory of the VMM process, which the
host kernel cannot reclaim without swap, so `memory.reclaim` frees almost
nothing from it. Sandboxes that can run above a reservation (those with
`resources.memoryMax` above `memory`, and those launched for memory
checkpoints) therefore boot with a virtio-balloon device:

- **`reclaim` inflates the balloon** by the bytes the guest uses above its
  reservation, and never past the point where the guest would have less than
  its reservation. The guest's balloon driver hands that memory back to the
  host. The device has `deflate_on_oom` set, so a guest that needs the memory
  back takes it before its own OOM killer would act: inflating can slow a
  guest down but never kills it.
- **When the episode ends**, the worker deflates the balloons it inflated, and
  the guest can burst again.
- **On cloud-hypervisor, free page reporting is on too.** A guest kernel with
  `CONFIG_PAGE_REPORTING` reports pages it frees, so the host gets them back
  continuously, with no pressure at all. Without that kernel option it is
  inert. Firecracker gets the balloon without free page reporting, because
  not every supported Firecracker release accepts the setting.

A Firecracker sandbox with a balloon boots with `--api-sock`, since the
balloon is driven over the API. Sandboxes without one keep `--no-api`.

### Configuration

Every field is optional. The block exists for tuning:

```nickel
resourceEnforcement = {
    enabled = true,
    oversubscribe = { memory = 1.5 },
    memoryPressure = {
        # Act when free memory (MemAvailable) drops below this.
        # Default: 5% of node memory, at least 256MiB, at most 4GiB.
        minFree = "2GiB",
        # ...or when tasks spend more than this share of time stalled
        # waiting on memory (kernel PSI "some", 10s average). Default 10%.
        maxStall = "10%",
        # After each step, wait this long for pressure to clear before
        # taking the next one. Default 5s.
        settle = "5s",
        # The steps this node may take, always in this order. Remove one
        # to forbid it. "evict-reserved" only ever runs when
        # oversubscribe.memory > 1. Default: all four.
        steps = ["reclaim", "suspend", "evict-bursting", "evict-reserved"],
        # Set false to keep enforcement but turn the response off.
        enabled = true,
    },
},
```

The manager validates the block when it loads the worker config: an unknown
step, a bad size or duration, or a `maxStall` outside 0-100% fails at
startup. `memoryPressure` without `enabled = true` is rejected, like
`oversubscribe`.

### See the order before it matters

The ordering is a pure function of usage, bands, and reservations, so the
worker computes it on every usage report and `legion node usage` prints it:

```console
$ legion node usage w-03
Node w-03 (ready)  usage sampled 4s ago

 RESOURCE  CAPACITY          RESERVED           USED             PRESSURE
 memory    98304 MiB ×1.5    134144 MiB (91%)   62464 MiB (42%)  ok (free 35840 MiB, stall 0.4%)
 cpu       64000 MHz         41200 MHz (64%)    18900 MHz (30%)

 ALLOCATION  JOB        INSTANCE  RES/MAX MiB  USED MiB  PEAK MiB  CPU   IDLE
 1042        workspace  user-42   512/4096     1830      1900      0.25  -
 1063        batch      -         400/2048     1900      1950      0.90  -
 1077        api        -         2048         1900      2010      0.40  -

UNDER PRESSURE, IN ORDER
 #  ALLOCATION      BAND    RES/USED MiB  STEP
 1  1063 batch      low     400/1900      reclaim, then evict-bursting
 2  1042 workspace  normal  512/1830      reclaim, then suspend
 3  1077 api        high    2048/1900     evict-reserved
```

Here `workspace` sets `suspend.onPressure = true`, so its row ends at
`suspend`: a suspended allocation is gone, and no later step reaches it.

The `PRESSURE` column reads `ok`, `UNDER PRESSURE` while a trigger fires, or
`response off`. An allocation that no step would act on (within its
reservation on a node without oversubscription, `critical` and within its
reservation, or node-pinned and not bursting) is not listed. The same order
is in the `GetNodeUsage` reply as `pressure_rank` and `pressure_steps` on
each allocation, and the response's status as `memory.pressure_response`.

### What it reports

Every action goes to the manager on the next heartbeat (`pressure_actions`)
and becomes a `node.pressure` event, warn for an eviction and info for
`reclaim`:

```console
$ legion events list --kind node.pressure
2026-10-10T14:02:11Z  WARN   node.pressure             [job=batch node=w-03 alloc=1063]  Allocation 1063: evicted by evict-bursting on w-03: using 1.9 GiB of a 400 MiB reservation while free memory was 1.1 GiB (threshold 2 GiB)
2026-10-10T14:02:05Z  WARN   node.pressure             [job=workspace node=w-03 alloc=1042]  Allocation 1042: suspended by suspend on w-03: using 1.8 GiB of a 512 MiB reservation while free memory was 1.3 GiB (threshold 2 GiB). Its state is checkpointed and it resumes with `legion instance start`
```

A `suspend` event's metadata carries `cause: suspended` and the
`snapshot_id` of the checkpoint, and the instance's own `instance.suspended`
event says `reason: pressure`.

The event's metadata carries the step, the numbers, `cause: node_pressure`,
and a `hint` for the job author, such as "raise resources.memory toward real
usage, or move this job to a pool without oversubscription". For an eviction
the manager also removes the allocation with `cause = node_pressure`, so the
task's stop notice, `allocation.stopping`, and the replacement eval all say
why. An OOM kill at the task's own `memoryMax` keeps its own annotation
("task exceeded its N MiB memory limit and was OOM-killed"), so "my own limit
or the machine?" always has an answer.

In the UI, the job page shows a "Memory pressure" card with each action
on that job's allocations, its detail sentence, and its hint, and the worker
page shows the same card for the node. Both read these events, so they need
`operator:read`.

The worker stops the task first and reports afterwards, because a node
running out of memory can't wait on the manager. The stop is the ordinary
graceful one (stop notice, signal, `stop.killAfter`). The worker holds the
allocation out of relaunch until the manager drops it from the desired set,
and a heartbeat that fails is retried with the same report.

Metrics: `legion_node_pressure_actions_total{node,step}` on the manager and
`legion_worker_pressure_actions_total{step}` on the worker. See
[metrics.md](metrics.md#node-usage).

### Limits of the response

- **`reclaim` needs Linux 5.19** for `memory.reclaim`. On an older kernel the
  write fails, the step frees nothing, and the response moves on to
  eviction.
- **`reclaim` on a sandbox needs its balloon.** A sandbox launched without
  one (no `memoryMax` above `memory`, not launched for memory checkpoints, or
  started before this release) falls back to `memory.reclaim`, which frees
  little from guest RAM.
- **The balloon is a request.** The guest driver gives back what it can, and
  `freed_bytes` reports the balloon's growth, not a measurement.
- **`suspend` needs the instance's node to take a checkpoint**: the worker's
  content store for a memory checkpoint, or a retained volume for a disk one.
  A suspend that cannot capture leaves the task running and the next step
  acts instead.
- **A one-second check can miss a fast spike.** A task that allocates
  gigabytes in under a second can still reach the kernel OOM killer first.
  The killer then acts on the cgroup at its `memory.max`, or on whatever the
  kernel picks when the whole node is out.
- **A replacement can land on the same node.** The scheduler has no memory
  of pressure, so an evicted allocation may be placed back on the node that
  evicted it if it is the best fit by reservations.
- **Only governed allocations are candidates.** Tasks on `noop` and on
  plugins that haven't opted in are never measured, never reclaimed, and
  never evicted.

## Failure modes

| Symptom | Cause | What to do |
| --- | --- | --- |
| Startup warning "no cgroup v2 unified hierarchy; tasks run unconfined" | The node runs cgroup v1, or the unified hierarchy isn't mounted | Boot with `systemd.unified_cgroup_hierarchy=1` (or your distro's equivalent), or leave enforcement off on that node |
| Startup warning "could not fingerprint any usable CPUs" | The worker's `cpuset.cpus.effective` is unreadable or empty | Check the worker's own cgroup placement. Enforcement stays off until the file is readable |
| Allocation fails: "cannot reserve N exclusive core(s) … this node has M free" | The scheduler's count said yes, but whole cores aren't actually free (fingerprint drift, offlined CPUs) | The message lists the free, reserved, and usable sets. The allocation goes through the normal `on_unhealthy` move handling instead of getting stuck |
| Allocation fails: "reserving N core(s) … would leave the share slice empty" | The reservation would take every usable CPU | Leave at least one core unreserved, or lower the reservation |
| Submit fails: "resources.cores … must be a whole number" | Fractional `cores` on a governed runtime | Round it, or declare `cpu` in MHz (for example `cpu = 500`) for a proportional share |
| Submit warns: "sets resources.cpus = 1, which is 1 MHz, not 1 CPU(s)" | `cpu` and its alias `cpus` are in MHz, and the value reads like a CPU count | Declare `cpu` in MHz (`cpu = 1000`), or `cores` for whole CPUs |
| Submit fails: "declares both resources.cores and resources.cpu" | Two CPU contracts on one task | Pick one |
| Submit fails: "resources.cores together with config.cgroup" | Legion can't fence an `exec` task that names its own cgroup | Drop `cores`, or drop `cgroup` and let the worker place the task |
| `legion node usage` says "no usage reported" | Enforcement is off on that worker, the worker predates usage reporting, no report has reached this manager since it started, or the node reports to another manager | Turn on `resourceEnforcement.enabled`, upgrade the worker, wait 10 seconds, or ask the manager the node reports to |
| `usage sampled` age keeps growing past 10s | The node stopped heartbeating, or stopped sending reports | Check `legion node list`. Reports older than 2 minutes are dropped and the node's gauges fall to zero |
| A sandbox is OOM-killed with plenty of guest memory free | The VMM needs more than `sandbox.vmmOverheadMiB` | Raise `sandbox.vmmOverheadMiB` on that worker type |
| Allocation fails: "failed to place task … within this node's resource limits" | A cgroup write failed (permissions, missing controller) | The wrapped error names the file. The reservation is rolled back, so no cores are left stranded |
| Submit fails: "resources.memoryMax (N MiB) is below resources.memory" | The ceiling is under the reservation, possibly a `memory` from `taskResourceDefaults` | Raise `memoryMax`, or lower `memory` |
| Submit warns: "sets resources.memoryMax, but the … runtime does not enforce resources" | `memoryMax` on `noop` or a plugin that hasn't opted in | Nothing caps the task. Use a governed runtime, or drop the field |
| Manager fails to start: "resourceEnforcement.oversubscribe and resourceEnforcement.memoryPressure need resourceEnforcement.enabled = true" | A ratio or a pressure block on a worker type without enforcement | Turn enforcement on, or remove the block |
| Worker warning "resourceEnforcement.oversubscribe is set but this node cannot enforce" | No cgroup v2 on that node | The node registers at physical capacity. Fix cgroup v2, or drop the ratio for that worker type |
| `node.pressure` events with `evict-bursting` for one job | The job's real usage is well above its `memory` | Raise `resources.memory` toward the `PEAK MiB` that `legion node usage` shows |
| `node.pressure` events with `evict-reserved` | An oversubscribed node ran out of physical memory | Lower `oversubscribe.memory`, move the job to a higher band, or to a pool without oversubscription |
| A task within its reservation is reclaimed anyway | An ancestor cgroup above `cgroupParent` caps `memory.low` protection | Mount cgroup v2 with `memory_recursiveprot`, or set `memory.low` on the ancestors |

## Interaction with the `exec` runtime's `cgroup` field

An `exec` task's `config.cgroup` works as it always has: the task joins a
cgroup that something else created and limited. It **takes precedence** over
worker-owned enforcement and removes the task from the Legion hierarchy. That's
why combining it with `cores` is rejected at submit, since Legion can't
guarantee exclusivity inside a cgroup it doesn't manage. `user`, `landlock`,
and `oom_score_adj` work with the worker-owned cgroup as before.

See [docs/task-runtime-exec.md](task-runtime-exec.md).

## Not implemented

- **Usage-aware placement.** The scheduler packs on reservations times the
  ratio, never on measured usage.
- **macOS.** Apple `container` has no CPU weight or pinning equivalent.
  `memory` and `cores` size the guest VM, and `cpu` (MHz) is used only for
  scheduling.
- **NUMA- and SMT-aware placement.** The allocator hands out whole logical
  CPUs with no topology awareness: no NUMA-node affinity and no avoidance of
  SMT siblings.
- **A non-root or delegated-subtree hierarchy root.** The current version
  assumes a root worker that creates its subtree at the cgroup root.
- **Fencing processes outside Legion**, and tasks on runtimes that haven't
  opted in.
- **The systemd cgroup driver** (`:slice:prefix:name` form). Legion writes the
  cgroupfs form.
