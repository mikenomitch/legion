# Resource Enforcement

On Linux, the `resources` a task declares can be what the **kernel** enforces,
not just what the scheduler counts. This page covers what each field means
when enforcement is on, how the worker partitions the node, what a task
runtime has to do to participate, and how it fails.

Enforcement is **opt-in per worker** and off by default. A declaration that
was only ever a scheduling estimate must not start OOM-killing workloads
because the cluster upgraded.

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
  exists);
- a worker that can create and write the `cgroupParent` subtree at the cgroup
  root — the standard root-worker deployment. A configurable root for
  delegated non-root setups is not implemented;
- the `cpuset`, `cpu`, and `memory` controllers available for delegation.

A worker that opts in but cannot enforce (no cgroup v2, no usable CPU
fingerprint) **warns at startup and runs tasks unconfined** rather than
failing every allocation. Enforcement must not be the reason a workload will
not run.

## What each field means

| Jobspec | Kernel meaning (Linux, enforcement on) |
| --- | --- |
| `resources.memory` (MiB) | A **hard** `memory.max`, with no swap allowance on top, so a leaking task cannot escape the limit by swapping. Exceeding it is a kernel OOM kill of *that task's* cgroup. (Written as OCI `memory.swap == memory.limit` for the container runtime and `memory.swap.max = 0` for a worker-created cgroup — cgroup v2 counts swap alone, the OCI spec counts memory+swap, so the two spellings mean the same rule.) |
| `resources.cores` (whole CPUs) | **Exclusive pinned CPUs.** The worker picks specific logical CPUs and removes them from every other governed task's CPU set. |
| `resources.cpu` (MHz) | A **proportional weight** (`cpu.weight`, via the standard `shares → weight` mapping with shares == MHz). It only bites under contention: an idle node lets everyone burst, a saturated one degrades each task in proportion to what it declared. |

Two rules follow, enforced at submit on every governed runtime:

- **`cores` must be a whole number.** A cpuset cannot hold half a CPU. (This
  was silently rounded on macOS and ignored on Linux before enforcement
  existed, so a `cores = 1.5` spec that used to be accepted now fails with a
  message naming both fixes.)
- **`cores` *or* `cpu`, never both.** They are two different CPU contracts —
  exclusive whole CPUs versus a share of the shared pool — and no coherent
  semantics combine them.

**Declared-only.** A job that declares nothing gets no memory or CPU limit and
is otherwise unconstrained; scheduler defaults are not folded in. It still
lands in the hierarchy, which is what makes another task's reserved cores
off-limits to it.

`resources.cpus` is the scheduler's historical alias for `cpu` (MHz), not for
`cores` — a `cpus = 0.5` declaration is a CPU share and is not treated as a
cpuset request.

## How the node is partitioned

The worker splits its usable CPUs into two parent cgroups it creates and owns
(Nomad's model):

```
legion/
├── share/     cpuset.cpus = usable − reserved     ← unpinned tasks live here
│   └── legion-<alloc>   cpu.weight from declared MHz
└── reserve/   cpuset.cpus = union of reservations
    └── legion-<alloc>   cpuset.cpus = its assigned cores, exclusively
```

A task declaring `cores` gets whole CPUs moved out of the share slice. Because
cgroup v2 cpusets are hierarchical, **one write to `legion/share/cpuset.cpus`
re-fences every tenant at once** — no per-task updates, and no window in which
a reserved core is still shared.

Usable CPUs are fingerprinted from the worker's own `cpuset.cpus.effective`,
not `nproc`: a worker that is itself confined must not hand out CPUs it cannot
use. Cores are assigned lowest-numbered-free first, released on stop or
destroy, and **persisted in the worker's allocation records** so a worker
restart rebuilds the allocator and both slice masks rather than handing a
running task's cores to somebody else.

The cgroup path is explicit and predictable —
`legion/share/legion-<alloc>` or `legion/reserve/legion-<alloc>` — so an
operator can find a task's cgroup by allocation id and see the share/reserve
split in the hierarchy.

### Exclusivity scope, stated honestly

The fence governs every runtime that opts in — today `container` and `exec`.
Processes **outside Legion entirely** (node daemons, SSH sessions) and tasks on
runtimes that have not opted in are beyond it and may still land on reserved
cores. That residue is inherent to running on a shared host; nothing short of
full-machine partitioning closes it.

Reserving *every* core is refused: every unpinned governed task on the node
lives in the share slice, so at least one core must remain in it.

## The runtime interface

Enforcement is worker-core, and task runtimes consume it through one defaulted
trait method plus a struct on the instance. Worker core owns everything hard —
fingerprinting, the allocator, both slice masks, persistence, restart
recovery, release — so an adopting runtime never touches accounting.

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
| `container` | `RuntimeApplied` | The assignment lands in the generated OCI spec (`cgroupsPath`, `linux.resources.memory`, `cpu.cpus` / `cpu.shares`) and the OCI runtime creates the cgroup at that path — inside the worker's hierarchy, so the share mask governs it. |
| `exec` | `WorkerCgroup` | Worker core creates and sizes the cgroup; the plugin joins it in its `pre_exec` sequence, before the switch to the task user. |
| `sandbox`, `noop`, dylib plugins | `Unsupported` | `resources` stay scheduling-only, as before. |

Because `container` and `exec` share one hierarchy, a core reserved by either
is exclusive against **both** — which is the only way "exclusive" can be true
of a node rather than of a runtime.

Adopting is two lines for a runtime whose task is a host process:

```rust
fn resource_enforcement(&self) -> ResourceEnforcement {
    ResourceEnforcement::WorkerCgroup
}

// …and at spawn:
join_assignment_cgroup(&mut cmd, instance)?;   // no-op when nothing is governed
```

`join_assignment_cgroup` writes the pid into the target cgroup from a
`pre_exec` hook, so the process is confined before it execs. A runtime that
needs the join at a specific point in its own privileged sequence (as `exec`
does, before dropping to the task user) can read
`instance.resource_assignment` directly instead.

See [docs/task-runtime-plugins.md](task-runtime-plugins.md) for the plugin
author's view, including the `ResourceAssignment` fields.

## What enforcement buys beyond limits

- **Whole-tree teardown.** A `WorkerCgroup` task's processes all live in a
  Legion-owned cgroup, so the hard-kill step of `stop` writes `cgroup.kill` —
  reaping double-forked descendants that a tracked-pid kill would orphan. This
  runs *after* the graceful stop ladder, so a task that drained cleanly is
  never SIGKILLed for the privilege.
- **OOM attribution.** On exit, the worker reads the cgroup's `memory.events`
  and, when the OOM counter fired, annotates the failure:

  ```
  exited with status 137; task exceeded its 256 MiB memory limit and was OOM-killed
  ```

  Exit code 137 on its own sends the author hunting through dmesg.

## Failure modes

| Symptom | Cause | What to do |
| --- | --- | --- |
| Startup warning "no cgroup v2 unified hierarchy; tasks run unconfined" | The node runs cgroup v1, or the unified hierarchy is not mounted | Boot with `systemd.unified_cgroup_hierarchy=1` (or the distro equivalent), or leave enforcement off on that node |
| Startup warning "could not fingerprint any usable CPUs" | The worker's `cpuset.cpus.effective` is unreadable or empty | Check the worker's own cgroup placement; enforcement stays off until it is readable |
| Allocation fails: "cannot reserve N exclusive core(s) … this node has M free" | The scheduler's count said yes but whole cores are not actually free (fingerprint drift, offlined CPUs) | The message names the free, reserved, and usable sets. The allocation feeds the normal `on_unhealthy` move machinery rather than wedging |
| Allocation fails: "reserving N core(s) … would leave the share slice empty" | The reservation would take every usable CPU | Leave at least one core unreserved, or lower the reservation |
| Submit fails: "resources.cores … must be a whole number" | Fractional `cores` on a governed runtime | Round it, or use `cpu` (MHz) for a proportional share |
| Submit fails: "declares both resources.cores and resources.cpu" | Two CPU contracts on one task | Pick one |
| Submit fails: "resources.cores together with config.cgroup" | An `exec` task naming its own cgroup cannot be fenced by Legion | Drop `cores`, or drop `cgroup` and let the worker place the task |
| Allocation fails: "failed to place task … within this node's resource limits" | A cgroup write failed (permissions, missing controller) | The wrapped error names the file. The reservation is rolled back, so cores are not stranded |

## Interaction with the `exec` runtime's `cgroup` field

An `exec` task's `config.cgroup` still means what it has always meant: join a
cgroup somebody else created and limited. It **wins** over worker-owned
enforcement and opts the task out of the Legion hierarchy — which is why
combining it with `cores` is rejected at submit: Legion cannot promise
exclusivity inside a cgroup it does not manage. `user`, `landlock`, and
`oom_score_adj` compose with the worker-owned cgroup unchanged.

See [docs/task-runtime-exec.md](task-runtime-exec.md).

## Not implemented

- **macOS.** Apple `container` has no weight or pinning analogue: `memory` and
  `cores` size the guest VM, and `cpu` (MHz) stays scheduling-only.
- **NUMA/SMT-aware placement.** The allocator hands out whole logical CPUs
  with no topology awareness — no NUMA-node affinity, no SMT-sibling
  avoidance.
- **A non-root or delegated-subtree hierarchy root.** v1 assumes a root worker
  creating its subtree at the cgroup root.
- **Fencing processes outside Legion**, and tasks on runtimes that have not
  opted in.
- **The systemd cgroup driver** (`:slice:prefix:name` form). Legion writes the
  cgroupfs form.
