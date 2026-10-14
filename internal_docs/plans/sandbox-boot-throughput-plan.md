# Plan: Sandbox Boot Throughput

> **Judgement calls:** [`calls/sandbox-boot-throughput-plan_calls.md`](../calls/sandbox-boot-throughput-plan_calls.md)

**Status: SHIPPED** (phases 1 to 3). Phase 4, the per-node ceiling on large
nodes, is open in [`TODO.md`](../TODO.md). The public record is
[`docs/sandboxes-at-scale.md`](../../docs/sandboxes-at-scale.md#measured-on-aws),
[`docs/task-runtime-sandbox.md`](../../docs/task-runtime-sandbox.md#how-fast-a-sandbox-boots),
and [`docs/session-mode.md`](../../docs/session-mode.md#picking-a-node); the
measured runs are in
[`benchmarks/aws/sandbox-boot/RESULTS.md`](../../benchmarks/aws/sandbox-boot/RESULTS.md).

This plan also records which of the parallel `perf/*` branches the work
stacked in, since they shipped as one change. Two of them have plans of
their own: [`rootfs-base-cache-plan.md`](rootfs-base-cache-plan.md) and
[`task-start-critical-path-plan.md`](task-start-critical-path-plan.md). The
session router changes are recorded in
[`calls/session-durability-and-routing-plan_calls.md`](../calls/session-durability-and-routing-plan_calls.md)
(entries 40 to 47).

## Problem

Session mode has the right shape for sandboxes at scale: a start goes to a
node with no write to the replicated store, the node admits it against its
envelope, and the managers learn about it from heartbeats. The numbers did
not show it. Measured on AWS (c8i.xlarge workers, nested virtualization,
cloud-hypervisor v51.2), on `origin/main`:

- One microVM session took 2.9 s from create to a guest that takes exec.
- Four 4-vCPU workers booted a burst of 160 at 1.6 a second, p99 76 s.
- At a sustained 20 starts a second, 69% were refused.

The control plane was not the reason. Each start paid for work that had
nothing to do with scheduling:

- **Every cloud-hypervisor guest panicked on its first boot.** CH probes a
  raw disk's format and then refuses guest writes to its sector 0; ext4's
  superblock write at mount failed, the kernel panicked, and `panic=1`
  rebooted it a second later.
- **The serial console** cost about 700 ms of every boot under nested
  virtualization: about 300 kernel lines, one VM exit per byte.
- **Every session rebuilt its root disk** (a registry pull, layer
  extraction, `mkfs`), because the disk carried the session's env.
- **The router sent concurrent creates to one node**, counting a start only
  once the node answered.
- **The worker serialized under churn**: a session's stop held the global
  task-manager lock through a round trip into the guest and two state-file
  writes; a process start time read scanned all of `/proc`; each launch
  forked the worker and created a thread.

## Prior art

- **Modal sandboxes v2** (2026): schedulers pick a worker from cached state
  and call it directly; workers are the source of truth; no data store on the
  create path. Median under 0.5 s to user code, a million in under a minute.
  Legion's session mode is the same shape. The lesson taken here is theirs
  too: once the control plane is out of the way, the tail is per-node
  contention (their `rtnl` lock).
- **Firecracker** quotes about 125 ms to guest user space and 150 microVMs a
  second per host, with a minimal kernel and no serial chatter: the console
  and kernel matter as much as the VMM.
- **E2B** starts from memory snapshots (about 80 ms). Legion's numbers here
  are cold boots; snapshot starts are a separate, opt-in layer (see the
  pre-warm stance below).
- **Kubernetes** gives pod startup an SLO of p99 5 s, image pulls excluded,
  and Kata containers add a VM boot to that.

## Jobs to be done

- **Platform engineer running agent sandboxes**: "When my product starts a
  sandbox per task, I want it ready for code in a few hundred milliseconds,
  and I want a burst of thousands to land in seconds, without sizing a warm
  pool."
- **Operator**: "When starts are slow, I want to see which stage of a start
  took the time on which node, and know which knob or node type fixes it."
- **Agent calling the API**: "When `StartSession` says running, I want to
  exec at once, not retry until the guest's agent shows up."
- **Capacity planner**: "I want to know what one node and one manager can
  do, so I can size a fleet for a target boot rate and resident count."

## Goals and non-goals

Goals:

- A cold sandbox boot that is bound by the guest kernel, not by Legion.
- A node's start rate bound by its CPU, and a cluster's by the sum of its
  nodes.
- `running` means usable.
- Every number measured on real KVM, before and after, with the tooling
  committed.

Non-goals:

- Warm pools, memory-snapshot starts, and pre-warm pushes. Per the project's
  stance, warming is an optimization layered on a good cold path; this plan
  makes the cold path good and measures it alone.
- A guest kernel of Legion's own. The kernel's size is the operator's; the
  public doc says what it costs.
- Any change to keyed instances' raft path.

## What exists today (before this work)

Session mode (`docs/session-mode.md`), edge starts (`Worker.StartSession`),
the session router (`src/manager/sessions.rs`), the sandbox runtime
(`src/task_plugins/sandbox.rs`, `microvm.rs`), the session index
(`src/storage/session_index.rs`), and `legion-loadgen --mode instances`,
which measured keyed lifecycles against simulated nodes only.

## Design, as the user sees it

One new jobspec field, on session tasks only:

```nickel
stateful = {
  mode = "session",
  envelope = { resources = { cpu = 30000, memory = 61440 } },
  placement = "fast",   # or "spread", the default
},
```

Everything else changes defaults and behavior, not surface:

- A sandbox session reports `running` once its guest's agent takes exec.
- cloud-hypervisor guests boot on `hvc0` with every disk declared raw;
  Firecracker guests boot `quiet` (`kernel_args = "loglevel=7"` restores the
  log).
- Each start logs its stages; `legion_task_start_stage_seconds` gains a
  `ready` stage.
- Sandbox root disks come from a per-node base cache
  ([`rootfs-base-cache-plan.md`](rootfs-base-cache-plan.md)).

## Phases

1. **The boot itself** (shipped): raw disks and the console; `running` on
   readiness; per-stage timings.
2. **The start path** (shipped): the base root disk cache; shared registry
   clients and one tag resolution per burst; the router's reservation,
   random ties, least-full fallback, and `placement`.
3. **The worker under churn** (shipped): stops off the lock and out of the
   state file, `/proc` start times, `posix_spawn` for VMMs, pooled launch
   threads, O(changes) heartbeats in the session index.
4. **The per-node ceiling on large nodes** (open): a 96-vCPU node boots
   about 50 a second with its CPUs mostly idle. Needs profiling on metal
   with symbols (lock hold times, spawn contention).

## Testing

- Unit tests for each change (the vsock readiness probe, `two_choices`,
  the least-full fallback, single-flight tag resolution, the credential
  fingerprint, the cgroup shim, `/proc` start times against `sysinfo`).
- `tests/session_integration.rs` and `tests/edge_boot_integration.rs`.
- Measurements: `tests/session_index_scale_measure.rs` (index memory and
  heartbeat cost at a million sessions), `pick_cost_measure` (router pick
  per placement and fleet size), and the AWS rig
  (`benchmarks/aws/sandbox-boot/`) with `legion-loadgen --mode sessions`.

## Rollout

No installed base, so no migration. A cloud-hypervisor checkpoint taken
before the disk change saved its disks without `image_type`, so a guest
restored from one can still hit the sector-0 refusal on its next superblock
write; take new checkpoints after upgrading. Tasks become process-group
leaders rather than session leaders. The PR description lists both.
