# Proposal: Shared Files Between Tasks in the Same Allocation

> **Status:** PROPOSAL — not implemented. Companion to the file-isolation
> work that made per-allocation directories owner-only (`0700`): that work
> answers "tasks on the same *node* must not read each other's files"; this
> proposal answers "should tasks in the same *allocation* be allowed to, and
> how".

## Context

Today an allocation holds exactly one task instance. Tasks in the same job
`group` co-locate on one node but each gets its own allocation — and, after
the isolation work, its own `0700` directory tree that no other task can
enter. The on-disk layout (`allocs/<alloc_id>/<task_name>/`) and the
`LEGION_ALLOC_DIR` env var already anticipate allocations that contain
multiple tasks (sidecars, init tasks). When that lands, the question becomes
unavoidable: a sidecar that cannot see *any* of the main task's files is
useless, but a sidecar that can see *all* of them re-opens the hole the
isolation work just closed.

## Position

**Yes — tasks in the same allocation should be able to share files, but only
through one explicit, purpose-built shared directory.** Private task
directories stay private even within an allocation.

The trust argument: tasks in one allocation come from one job spec, written
by one author, admitted under one set of ACLs — they are a single trust
*domain*. Sharing a scratch space within that domain is safe and is the whole
point of co-scheduling. But they are not a single trust *unit*: a sidecar
built from a third-party image (log shipper, proxy) is the most likely
component to be compromised, so secrets and private state must not be
ambiently visible to it. "Shared by request, private by default" serves both.

Concretely, the situations that justify sharing — and shape the design:

- **Log/metric shipper sidecars** tailing output the main task writes.
- **Init/prestart tasks** that fetch or build inputs the main task consumes.
- **Proxy/cert sidecars** handing rotated certificates to the main task.
- **Pipelines** where co-located tasks pass intermediate files.

## Design

### Layout

```
allocs/<alloc_id>/
├── shared/        # read-write for every task in the allocation
├── <task_a>/      # private to task_a (0700, task_a's user)
└── <task_b>/      # private to task_b (0700, task_b's user)
```

- New env var `LEGION_SHARED_DIR` for every task in the allocation.
  `LEGION_ALLOC_DIR` keeps pointing at the alloc root; `LEGION_TASK_DIR`
  keeps pointing at the private dir.
- The alloc root stays `0700` against the rest of the node — the shared dir
  loosens nothing outside the allocation.

### Permission mechanics (exec kind)

- **All tasks run as the worker's user (default):** everything in the
  allocation is one uid; `shared/` is a path convention, enforcement is
  moot. This is the common dev/single-tenant case.
- **Per-task `user`:** private dirs are chowned to their task's user (as
  shipped today). `shared/` gets a per-allocation group: the worker (running
  as root, which per-task `user` already requires) creates a transient group
  per allocation, sets `shared/` to `2770` (setgid) with that group, and
  adds each task's supplementary groups at spawn. Files created by any task
  land group-readable for the others.
- **Worker not privileged enough for groups:** refuse `user` + shared-dir
  jobs at admission rather than silently degrading to world-writable modes.

### Sandbox and kind integration

- **Landlock (exec):** `shared/` rides along into `read_write_paths`
  automatically, exactly as the private task dir does today. Other tasks'
  private dirs are never added.
- **Container kind:** bind-mount `shared/` at a fixed in-container path
  (e.g. `/legion/shared`) once volume plumbing lands. Private task dirs are
  never mounted into other tasks' containers.

### Secrets stay private — no exceptions

Identity tokens, KV file injection, and any worker-delivered credential
always materialize in the *private* task dir of the task that declared them.
A job that wants a sidecar to hold a credential declares the identity on the
sidecar. This keeps "compromised sidecar reads the main task's tokens" off
the table by construction.

### Job spec surface

- Default **on** for multi-task allocations (matching Nomad's `alloc/` and
  user expectations for sidecars); single-task allocations skip creating it.
- `sharedDir: false` at the group level opts out for jobs that co-schedule
  mutually distrustful tasks.

## Non-goals

- **Cross-allocation sharing** (even same job, same node): that is what
  volumes are for — see the
  [volume plugin system plan](volume-plugin-system-plan.md).
- **Granting one task access to another's private dir**, in any
  configuration. If two tasks need a common file, it goes in `shared/`.
