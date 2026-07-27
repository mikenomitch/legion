# Judgement Calls — Shared Alloc Directory

Companion to
[`plans/shared-alloc-dir-proposal.md`](../plans/shared-alloc-dir-proposal.md)
(status: proposal, not implemented).

## 1. Share by request, private by default — one explicit shared directory

Tasks in the same allocation may share files, but only through a purpose-built
`shared/` directory; private task dirs stay private *even within an allocation*.
The reasoning: tasks in one allocation come from one job spec, one author, one set
of ACLs — a single trust **domain**, so a shared scratch space is safe and is the
whole point of co-scheduling. But they are not a single trust **unit**: a sidecar
built from a third-party image (log shipper, proxy) is the most likely component
to be compromised. The two rejected extremes are "sidecar sees nothing" (makes
sidecars useless) and "sidecar sees everything" (reopens the hole the file
isolation work just closed).

## 2. Secrets never enter the shared directory — no exceptions

Identity tokens, KV file injection, and any worker-delivered credential always
materialize in the *private* dir of the task that declared them. A job that wants
a sidecar to hold a credential declares the identity on the sidecar. This keeps
"compromised sidecar reads the main task's tokens" off the table by construction
rather than by policy.

## 3. Refuse rather than silently degrade when the worker can't enforce it

Per-task `user` requires a privileged worker to create the per-allocation group
and setgid `shared/` to `2770`. If the worker isn't privileged enough, `user` +
shared-dir jobs are **rejected at admission** instead of falling back to
world-writable modes. A silent permissions downgrade is the worst possible
outcome for a feature whose entire justification is a security boundary.

## 4. Default on for multi-task allocations, opt out per group

Matches Nomad's `alloc/` and user expectations for sidecars; single-task
allocations skip creating it entirely. `sharedDir: false` at the group level is
the opt-out for jobs that deliberately co-schedule mutually distrustful tasks.

## 5. Cross-allocation sharing stays a non-goal

Even same job, same node. That is what volumes are for — see the
[volume plugin system plan](../plans/volume-plugin-system-plan.md). Likewise,
nothing grants one task access to another's private dir in any configuration; if
two tasks need a common file it goes in `shared/`.
