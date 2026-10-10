# Resource Enforcement

On Linux, the kernel can enforce the `resources` a task declares, instead of
the scheduler only counting them. This page covers what each field means when
enforcement is on, how the worker divides up the node, what a task runtime
must do to take part, and how enforcement fails.

Enforcement is **opt-in per worker** and off by default. A declaration that
was only ever a scheduling estimate shouldn't start getting workloads
OOM-killed just because the cluster was upgraded.

## Turning it on

```nickel
# configs/workers/default.ncl
{
    resourceEnforcement = {
        enabled = true,
        # Hierarchy root under /sys/fs/cgroup. Legion creates `share/` and
        # `reserve/` beneath it, and one cgroup per allocation beneath those.
        cgroupParent = "legion",
    },
}
```

Requirements:

- the **cgroup v2 unified hierarchy** (`/sys/fs/cgroup/cgroup.controllers`
  exists),
- a worker that can create and write the `cgroupParent` subtree at the cgroup
  root, which is the standard setup when the worker runs as root. A
  configurable root for delegated non-root setups is not implemented.
- the `cpuset`, `cpu`, and `memory` controllers available for delegation.

If a worker opts in but can't enforce (no cgroup v2, or no usable CPU
fingerprint), it **logs a warning at startup and runs tasks unconfined**
instead of failing every allocation. Enforcement shouldn't be the reason a
workload won't run.

## What each field means

| Jobspec | Kernel meaning (Linux, enforcement on) |
| --- | --- |
| `resources.memory` (MiB) | A **hard** `memory.max`, with no swap allowance on top, so a leaking task can't get around the limit by swapping. Going over it triggers a kernel OOM kill of *that task's* cgroup. (The container runtime writes this as OCI `memory.swap == memory.limit`, and a worker-created cgroup gets `memory.swap.max = 0`. Cgroup v2 counts swap on its own while the OCI spec counts memory plus swap, so both express the same rule.) |
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

`resources.cpus` is the scheduler's older alias for `cpu` (MHz), not for
`cores`. A `cpus = 0.5` declaration is a CPU share, not a cpuset request.

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

The fence covers every runtime that opts in, which today means `container`
and `exec`. Processes **outside Legion** (node daemons, SSH sessions) and tasks
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
| `sandbox`, `noop`, dylib plugins | `Unsupported` | `resources` are used only for scheduling. |

`container` and `exec` share one hierarchy, so a core reserved by either one
is exclusive against **both**. That's what makes cores exclusive across the
node and not only within one runtime.

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

## Failure modes

| Symptom | Cause | What to do |
| --- | --- | --- |
| Startup warning "no cgroup v2 unified hierarchy; tasks run unconfined" | The node runs cgroup v1, or the unified hierarchy isn't mounted | Boot with `systemd.unified_cgroup_hierarchy=1` (or your distro's equivalent), or leave enforcement off on that node |
| Startup warning "could not fingerprint any usable CPUs" | The worker's `cpuset.cpus.effective` is unreadable or empty | Check the worker's own cgroup placement. Enforcement stays off until the file is readable |
| Allocation fails: "cannot reserve N exclusive core(s) … this node has M free" | The scheduler's count said yes, but whole cores aren't actually free (fingerprint drift, offlined CPUs) | The message lists the free, reserved, and usable sets. The allocation goes through the normal `on_unhealthy` move handling instead of getting stuck |
| Allocation fails: "reserving N core(s) … would leave the share slice empty" | The reservation would take every usable CPU | Leave at least one core unreserved, or lower the reservation |
| Submit fails: "resources.cores … must be a whole number" | Fractional `cores` on a governed runtime | Round it, or use `cpu` (MHz) for a proportional share |
| Submit fails: "declares both resources.cores and resources.cpu" | Two CPU contracts on one task | Pick one |
| Submit fails: "resources.cores together with config.cgroup" | Legion can't fence an `exec` task that names its own cgroup | Drop `cores`, or drop `cgroup` and let the worker place the task |
| Allocation fails: "failed to place task … within this node's resource limits" | A cgroup write failed (permissions, missing controller) | The wrapped error names the file. The reservation is rolled back, so no cores are left stranded |

## Interaction with the `exec` runtime's `cgroup` field

An `exec` task's `config.cgroup` works as it always has: the task joins a
cgroup that something else created and limited. It **takes precedence** over
worker-owned enforcement and removes the task from the Legion hierarchy. That's
why combining it with `cores` is rejected at submit, since Legion can't
guarantee exclusivity inside a cgroup it doesn't manage. `user`, `landlock`,
and `oom_score_adj` work with the worker-owned cgroup as before.

See [docs/task-runtime-exec.md](task-runtime-exec.md).

## Not implemented

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
