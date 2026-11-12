# Plan: Session Starts from a Per-Node Snapshot

> **Judgement calls:** [`calls/session-snapshot-start-plan_calls.md`](../calls/session-snapshot-start-plan_calls.md)

**Status: PLANNED** (written 2026-11-12). Nothing is built. It follows the
sandbox boot work ([`sandbox-boot-throughput-plan.md`](sandbox-boot-throughput-plan.md)),
whose non-goals deferred snapshot starts until the cold path was measured
good (180 to 230 ms a boot, which it now is).

## Problem

A sandbox session cold-boots: a root disk clone, a VMM spawn, a guest kernel
boot, and init, about 180 to 230 ms to a guest that takes exec on AWS, most
of it the guest kernel. Products that start a sandbox per task (agent
workspaces, code interpreters) also pay the workload's own warm-up on every
start: a Python interpreter importing its standard library, a language
server indexing, a JIT warming. A restore from a memory snapshot skips both:
the guest wakes up mid-instruction with its page cache, its interpreter, and
its imports already in memory.

Legion can already restore a guest into a new instance with an identity of
its own (a fork, or `CreateInstance` with `from_snapshot`): a new TAP and
MAC, a new identity disk with its instance id, env, and token, fresh
entropy, and the clock stepped forward. Sessions cannot use it:
`from_snapshot` is refused for a session, and nothing on a node makes or
keeps a snapshot for starts to come from.

## Prior art

- **E2B** starts sandboxes from memory snapshots of a template (about 80 ms
  to a running sandbox), built once per template and shared across hosts.
- **Firecracker snapshots** (Lambda SnapStart, Fly machines' suspend) restore
  a guest from a memory file and a device state file. Lambda reseeds
  entropy and lets the runtime run `afterRestore` hooks, because a template's
  RNG state and connections are shared by every restore.
- **Modal** restores containers from memory snapshots taken after the
  user's import phase (`enable_memory_snapshot`), opt-in per function,
  falling back to a cold start when the snapshot is not usable.
- **Kubernetes** has no equivalent; checkpoint/restore (CRIU) is for
  forensics and migration, not starts.

The common shape: opt-in, a template per image or function taken after its
warm-up, a fresh identity and entropy per restore, and a cold start whenever
the snapshot cannot be used.

## Jobs to be done

**Job author / app developer**
- When my product starts a sandbox per user request, I want it ready with
  its interpreter already warm, so the user's first command runs in tens of
  milliseconds, not after a boot and an import phase.
- When I change my image or my task, I want new sessions to stop using the
  old snapshot without me deleting anything.

**Cluster operator**
- When I turn this on, I want to see how many starts came from a snapshot
  and how many booted, and why a snapshot was not used, so I can tell
  whether it is working on a node.
- I want a bound on the disk the snapshots take, the same budget as the
  rest of the node's derived content.

**Security / platform admin**
- When sessions start from one snapshot, I need each to have its own
  identity, token, entropy, and network address, and nothing from the
  template's run (no env of another session, no secrets), so a session can
  never read another's data out of shared memory.

**On-call responder**
- When a session started from a snapshot misbehaves, I want its start log
  line to say it was restored and from which template, so I can rule the
  snapshot in or out.

## Goals and non-goals

Goals:

- An opt-in session start from a per-node memory snapshot of the task's
  guest, taken once per node per task config, that falls back to a boot
  whenever the snapshot is missing, building, or fails to restore.
- Each restored session is a fork: its own identity, env, token, entropy,
  MAC, and address, exactly as a fork of a keyed instance gets.
- No template ever carries a session's data.

Non-goals:

- Sharing templates across nodes (publishing them). Each node builds its
  own; a published template is a later phase if one node's build cost
  matters.
- Warm pools of booted guests. A snapshot start is the same cold path with
  the boot replaced by a restore, so it needs no capacity held idle.
- Snapshots taken after user code ran (Modal's "after imports"). The
  template is the task's guest as its own command leaves it once ready.

## What exists today

- Session launches run off the task-manager lock (`UnlockedLaunch`), and a
  launch with `restore` set restores instead of starting.
- Fork restores give a guest a new identity: `ensure_fork_tap`,
  `write_identity_disk` with a `ResumeIdentity` (instance id, env, token,
  MAC, entropy, host time), and the guest's `legion guest-resume` handler.
- Session suspend and resume keep a session's own memory checkpoint on its
  node (`session_sleep`), and restores can be lazy on cloud-hypervisor v52+
  (`memory_restore`).
- The node content store's derived class keeps per-node built content
  (root disk bases) with a budget and LRU eviction.

## Design, as the user sees it

One field on session tasks:

```nickel
stateful = {
  mode = "session",
  envelope = { resources = { cpu = 30000, memory = 61440 } },
  start = "snapshot",   # default "boot"
},
```

With `start = "snapshot"`:

1. When a node first runs the task's envelope (or the first session start
   finds no template), the node boots one **template guest** of the task
   with no session: no session id, a placeholder env, no token. Once its
   agent takes exec, the node takes a memory checkpoint with the guest
   stopped after (`then = stop`) and keeps it as derived content keyed by
   the task config, the image digest, the guest binary, the kernel, and the
   VMM and its version.
2. A session start restores the template as a fork, with the session's
   identity disk. Starts that arrive while the template builds boot, as
   they do today.
3. Any failure to restore boots instead, with the reason in the start's log
   line and the metric.

```text
INFO Task allocation started allocation_id=-412 task_name=workspace task_runtime=sandbox
  start=snapshot template=rootfs/3f1c… total_ms=38 stages="restore=31ms ready=4ms register=1ms"
```

Metrics: `legion_session_starts_total{task, via="snapshot|boot", reason}`.

Refused at submit: `start = "snapshot"` on a task whose runtime cannot take
memory checkpoints (`stateful.start = "snapshot" needs a runtime with memory
checkpoints; task "workspace" runs on "container"`).

## Phases

1. **Template and restore on cloud-hypervisor**, non-gated sessions with no
   volumes: the field, the template guest, the derived-content entry and its
   key, the fork restore through the session launch path, the fallback, the
   metric and the log line. Ships alone behind the opt-in field.
2. **Gated and TAP sessions**: the template is taken with its network held
   shut, and each restore wires its own namespace or TAP, as forks do.
3. **Firecracker**, whose snapshots restore only on the node and VMM version
   that took them (already true of a per-node template).
4. **Prebuilt on envelope placement**, so the first session on a node does
   not boot: the node builds the template when it first runs the envelope.

## Testing

- Unit: the template key changes with each input; a start falls back to a
  boot with the right reason when the template is missing, building,
  incompatible, or fails to restore; a template guest never receives a
  session's env.
- Integration (fake VMM APIs, as the fork tests use): a session restored
  from a template gets its own identity disk, MAC, and entropy.
- Real KVM, on the sandbox-boot rig: start time and boots a second with
  and without the template, on a 4-vCPU and a 96-vCPU node, recorded in
  `benchmarks/aws/sandbox-boot/RESULTS.md` before the default is discussed.

## Rollout

Opt-in per task, default `boot`, no migration. A template is per node and
rebuilt when any key input changes, so an upgrade of Legion, the kernel, or
the VMM costs one boot per task per node.
