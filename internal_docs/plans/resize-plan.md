# Plan: Resize: One Vertical-Scaling Primitive for Every Unit

> **Judgement calls:** [resize-plan_calls.md](../calls/resize-plan_calls.md)

**Status: PLANNED** (written 2026-10-28). Nothing here is built.

Today a running unit's size can change only through its ceiling
(`memoryMax`) or by replacing it with a deploy. This plan adds **resize**:
change a running allocation's, session's, or isolate's `memory`,
`memoryMax`, or `cpu` in place, as a runtime override over the spec, the
same way `legion job scale` overrides `count`. Inside an envelope a resize
is node-local and writes nothing to raft; for a top-level allocation it is
one scheduler-lane compare-and-set, the shape elastic envelopes already
use. When the node cannot fit the new size, a resize is refused, or, for a
unit that can move, moves it.

It is the actuator the [autoscaler](autoscaler-v2-plan.md) deferred
("Vertical scaling (changing `resources`)"), and the way an
[isolate](isolate-runtime-plan.md)'s VM or any
[child session](child-sessions-plan.md) grows after it starts.

Deliberately deferred, and named in §4: memory hotplug beyond a VM's boot
size, resizing `cores` (cpusets), resizing GPUs and devices, and any
policy that decides when to resize (that is the autoscaler's).

---

## 1. Problem statement

1. **Size is fixed at start.** A session that turns out to need 4 GiB
   instead of 1 must be stopped and started again bigger, losing its
   state, or have been reserved at 4 GiB all along.
2. **The ceiling is not the reservation.** `memoryMax` lets a task burst
   above its reservation, but the burst is reclaimed first under pressure
   (`docs/resource-enforcement.md`, "Burstable memory"). A unit that needs
   the memory for an hour has no way to say "reserve this now".
3. **Editing `resources` is a deploy.** It creates a new job version and a
   rollout that replaces every allocation. The autoscaler v2 plan defers
   vertical scaling partly because nothing smaller exists to call.
4. **Envelopes already resize; nothing else does.** Elastic envelopes
   rewrite their allocation row in one scheduler-lane entry
   (`src/storage/store/envelope_ops.rs:80`,
   `src/manager/sessions.rs:1724`), with a `from`/`to` compare-and-set
   (`EnvelopeResize`, `proto/legion.proto:6135-6150`). The same mechanism
   is not available for the units inside them, or for ordinary
   allocations.

## 2. Prior art

- **Kubernetes in-place pod resize**
  ([docs](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/)).
  Stable since v1.35. A container's requests and limits change through the
  pod's `resize` subresource; the kubelet updates cgroups in place, and
  reports `PodResizePending` with reason `Infeasible` (cannot fit this node)
  or `Deferred` (retried later, highest priority first) in the pod's
  conditions. Scheduler preemption for deferred resizes is alpha in v1.37.
  The Vertical Pod Autoscaler can apply recommendations this way instead of
  evicting.
  - **Take:** resize as its own operation on a running unit, reported as
    applied, deferred, or infeasible, separate from the spec. Legion's
    `deferred` is narrower: only while an elastic envelope is growing.
  - **Reject:** a per-container resize policy matrix (`NotRequired` /
    `RestartContainer` per resource). Legion's runtimes say what they can do;
    the caller does not configure it.
- **Nomad.** Changing `resources` is a destructive update: a new
  allocation replaces the old one.
  - **Reject:** as the only path.
- **Borg Autopilot** ([EuroSys '20](https://research.google/pubs/autopilot-workload-autoscaling-at-google-scale/)).
  Vertical autoscaling: Autopilot sets the CPU and memory limits of
  individual tasks from usage histograms, for most of Google's fleet.
  - **Take:** vertical scaling is mostly a policy over an in-place
    primitive; this plan builds the primitive.
- **cloud-hypervisor** resizes vCPUs and memory of a running VM through its
  API (`vm.resize`) when the VM booted with headroom; both VMMs Legion uses
  have a virtio-balloon to give memory back within the boot size.
  - **Take:** the balloon in Phase 2; hotplug later.
- **Legion's own count overrides** (`legion job scale`,
  [docs/autoscaling.md](../../docs/autoscaling.md)): a separate record over
  the spec, no new version, the `scale` ACL resource, idempotency keys, and
  submit arbitration that drops a mismatched override.
  - **Take:** the whole shape, for resources.

## 3. Jobs to be done, by persona

**Job author / agent platform builder**
- When a session I started small turns out to need more memory, I want to
  give it more without restarting it, so its user does not lose their work.
- When an isolate's VM is about to compile something big, I want it to ask
  for more memory for the next ten minutes and give it back after, so I do
  not reserve the peak all day.

**Cluster operator**
- When a service is clearly over-reserved, I want to shrink its
  reservation without a deploy, so the room goes back to the scheduler
  today.
- When a resize cannot fit, I want to be told why and what would fit, so I
  can move the unit or make room deliberately.

**Autonomous agent / the autoscaler app**
- When my policy decides a task needs more, I want one idempotent call per
  unit or per task with a structured outcome, so I can act on `refused`
  without guessing.

*Not in scope:* the on-call responder gets no new surface beyond seeing an
allocation's effective resources and its resize events.

## 4. Goals and non-goals

### Goals

- One operation, resize, for every unit: top-level allocations, sessions,
  isolates, and child sessions, over `memory`, `memoryMax`, and `cpu`.
- No new job version and no rollout for a resize.
- Node-local (no raft) inside an envelope; one compare-and-set for a
  top-level allocation.
- Never strand a unit: a grow reserves before it applies; a shrink applies
  before it releases.
- Each runtime says what it can change live; anything else is refused with
  a named reason, never applied by a silent restart.

### Non-goals

- **Deciding when to resize.** That is the autoscaler's job.
- **Memory hotplug** beyond a VM's boot size (cloud-hypervisor can; it needs
  the guest booted with headroom). Deferred to §11.
- **`cores`** (exclusive cpusets) and devices.
- **Preempting other work to make room.** A grow takes free room only, as
  an envelope grow does.
- **Changing a runtime** (an isolate becoming a VM). That is not a size
  change; see [child sessions](child-sessions-plan.md) and calls entry 6.

## 5. Background: what exists today

- **Reservation and ceiling.** `resources.memory` is what the scheduler packs
  on and the cgroup's `memory.low`; `resources.memoryMax` is the hard
  `memory.max`; `cpu` is `cpu.weight` (docs/resource-enforcement.md, "What
  each field means"). A sandbox boots its guest at `memoryMax` (else
  `memory`) and its cgroup gets the guest plus `vmmOverheadMiB`.
- **Balloons.** A sandbox whose `memoryMax` exceeds its reservation boots
  with a virtio-balloon (`docs/task-runtime-sandbox.md`, "Guest memory under
  pressure"), which the pressure response inflates to reclaim.
- **Envelope resize.** Node-asked, manager-applied, one scheduler-lane
  entry, `from`/`to` compare-and-set, grows take only free room and never
  preempt, and shrinks are capped on the node before they are asked for
  (`docs/session-mode.md`, "Elastic envelopes").
- **Count overrides.** `legion job scale` writes a `task_scale` record,
  gated by the `scale` ACL resource ("so a principal can resize without
  redeploy rights", `docs/auth-and-acls.md`), and submit arbitration drops
  an override that no longer matches the spec.

## 6. Proposed design

### 6.1 The ladder

From cheapest to most expensive, and what each costs:

| Step | What changes | Raft | Exists |
| --- | --- | --- | --- |
| 1. Use up to the ceiling | nothing: the unit uses memory between `memory` and `memoryMax` | none | yes (`memoryMax`) |
| 2. Resize in place | the unit's reservation and/or ceiling | none inside an envelope; one CAS for a top-level allocation | **this plan** |
| 3. Move | the unit suspends and resumes on a node with room, at the new size | as for a resume | the move exists for `store` sessions and keyed instances; resizing on the way is **this plan** |

### 6.2 What a resize is

A resize sets the effective `memory`, `memoryMax`, and `cpu` of one unit:

- **A unit inside an envelope** (a session, an isolate, a child session):
  the node checks the envelope's free room for a grow, admits it, and tells
  the runtime. An elastic envelope that would overflow grows through its
  existing path, and the resize answers `RESOURCE_EXHAUSTED` (retryable)
  until it has. Nothing reaches raft.
- **A top-level allocation:** the manager applies a scheduler-lane
  compare-and-set on the allocation row (`from` the effective resources it
  believes are committed, `to` the new ones), checking the node's free room
  for a grow; then the worker applies it.

**Order.** A grow reserves first (envelope admission, or the CAS) and then
applies on the node. A shrink applies on the node first (lowering the
cgroup ceiling, inflating the balloon, lowering the isolate's cap) and then
releases the room. A shrink below current use is refused unless `--force`,
which may reclaim or kill per the runtime.

### 6.3 What each runtime can change

A runtime declares its live-resize support (`TaskPlugin::resize_support`)
and applies a resize (`TaskPlugin::resize`):

| Runtime | `memory` (reservation) | `memoryMax` (ceiling) | `cpu` |
| --- | --- | --- | --- |
| `exec`, `container` | live (`memory.low`) | live (`memory.max`; a shrink below use is refused) | live (`cpu.weight`) |
| `sandbox` | live (`memory.low`, scheduler) | down: balloon inflates; up to the boot size: balloon deflates; above the boot size: `needs_restart` | live for the VMM's threads; guest vCPUs unchanged |
| `isolate` | live (envelope admission) | live (the pooling limiter), up to the cordon's slot size | live (the shard scheduler's weight) |
| `envelope` | the existing elastic path | n/a | the existing elastic path |
| others | refused, `not_supported` | | |

`needs_restart` and `not_supported` are answers, never actions: the caller
can restart or `--move`, and the runtime never does either on its own.

### 6.4 The surface

```console
$ legion instance resize user-42 --memory 2GiB --memory-max 4GiB -j
{"ok":true,"data":{"instance":"user-42","node":"w-17","from":{"memory":1024,"memoryMax":2048},
 "to":{"memory":2048,"memoryMax":4096},"outcome":"applied","deduplicated":false}}

$ legion alloc resize 88213 --cpu 2000 --reason "batch week"
alloc 88213: cpu 1000 -> 2000 (override active; spec 1000)

$ legion instance resize ws-7 --memory-max 16GiB -j
{"ok":false,"error":{"code":"needs_restart",
 "message":"sandbox ws-7 booted with a 4096 MiB guest; firecracker cannot grow it past that while it runs",
 "hint":"restart the instance to boot at the new size, or pass --move to suspend it and resume it at 16GiB on a node with room",
 "retryable":false}}

$ legion alloc resize 88213 --memory 64GiB -j
{"ok":false,"error":{"code":"resource_exhausted",
 "message":"node w-03 has 11.2GiB free; the grow needs 63GiB more",
 "hint":"resize to at most 12GiB here, or move the allocation","retryable":true}}

$ legion job resize web --memory 1GiB            # Phase 3: every allocation of the task
$ legion job resize web --reset                  # back to the spec
```

`instance resize` takes keyed and session ids (a session's resize routes
by its node tag, `Worker.ResizeSession`). Every form takes `--reason`,
`--idempotency-key`, `-j`, and `--dry-run`, which answers whether it would
fit without applying it. The isolate's VM handle exposes the same call
(`machine.resize`, [isolate plan §7.4](isolate-runtime-plan.md)).

**Effective resources are visible everywhere resources are.** An
allocation's detail shows spec and effective resources and who changed
them; events `allocation.resized` and `instance.resized` carry `from`,
`to`, and `reason`; `legion node usage` and placement read effective
resources.

**ACL:** the existing `scale` resource (`scale:write` on the job), whose
doc already says it exists "so a principal can resize without redeploy
rights".

### 6.5 Overrides and the spec

- A resize of one unit is an override on that unit, kept with it: on the
  allocation row (top-level), or in the node's session table (sessions and
  isolates). It ends with the unit.
- A task-level resize (`job resize`, Phase 3) is a record beside the count
  override, layered over the spec, applied to each allocation in place
  where it fits and by the normal replacement path where it does not.
- **A submit that changes the task's `resources` drops the override,** the
  same arbitration count overrides follow: the author's new spec wins.

### 6.6 When it does not fit: move

`--move` resizes by moving, for units that can move: a session with
`suspend.durability = "store"`, or a keyed instance with a `suspend` block.
The unit suspends, its new size is recorded, and it resumes on a node with
room through the existing paths. Without `--move`, a grow that does not
fit is refused with the room the node has, so the caller decides.

## 7. Proto, struct, config, and persistence changes

- **Proto:** `ResizeAllocation`, `ResizeInstance`, `ResizeTask` (Phase 3),
  `Worker.ResizeSession`; `ResizeOutcome { applied | deferred | refused |
  needs_restart | not_supported }` with the node's free room on refusal;
  effective resources on allocation and instance messages. Full proto
  comments.
- **Store:** `resources_override` on the allocation row; a task-level
  override record beside `task_scale` (Phase 3). One migration adds the
  columns.
- **Worker:** the override in `sessions.db` for node-owned units.
- **Runtime trait:** `resize_support()` and `resize()`, defaulting to
  `not_supported`.
- **Catalog and MCP:** `alloc resize`, `instance resize`, `job resize`
  (`mutates: true`), MCP tools behind `--allow-write`;
  `docs/agent-readiness.md`.
- **ACL table** entries for each new Manager RPC.

## 8. Implementation phases

- **Phase 1: units, in place.** Resize for sessions, isolates, and child
  sessions (node-local), and for top-level `exec` and `container`
  allocations (the CAS); the runtime trait; `instance resize`, `alloc
  resize`, `--dry-run`; events; effective resources in the API and UI.
- **Phase 2: sandboxes.** Reservation changes, and the ceiling within the
  boot size through the balloon, on both VMMs.
- **Phase 3: tasks.** `job resize` and `--reset`: the task-level override,
  in-place where it fits, replacement where it does not, and submit
  arbitration.
- **Phase 4: move.** `--move` for movable units.
- **Then:** the autoscaler's vertical strategies call `ResizeTask` and
  `ResizeInstance` (autoscaler-v2 plan, its own phase).

## 9. Testing plan

- **Unit:** grow-reserve-then-apply and shrink-apply-then-release ordering;
  CAS conflicts answered `stale`; envelope admission for a session grow;
  per-runtime support table; override arbitration on submit.
- **Integration (`tests/integration/resize.rs`):** resize an `exec`
  allocation and read the cgroup files; refuse a grow past the node's free
  room with the room in the error; resize a session through its node tag;
  idempotent retries; a submit that changes `resources` drops the
  override; ACL entries.
- **KVM (ignored by default, Phase 2):** balloon down and back up on both
  VMMs with the guest's view of its memory checked; `needs_restart` above
  the boot size.

## 10. Rollout and backward compatibility

New operations only. Nothing resizes unless asked. Runtimes that do not
implement the trait answer `not_supported`. A worker without the
capability is never sent a resize; the manager answers `not_supported`
naming the worker version.

## 11. Open questions

1. **Memory hotplug beyond the boot size.** cloud-hypervisor supports it
   when the guest boots with headroom, which costs guest memory metadata for
   the reserved range. Lean: a later phase, opt-in per task, after Phase 2
   shows how often `needs_restart` is hit.
2. **Should a session's resize survive its suspend and resume?** Lean: yes;
   the size is part of the session, and a resume at the old size would
   surprise.
3. **Should grows ever preempt lower-priority work?** Lean: no, as for
   envelopes; the caller moves the unit instead.

## 12. Acceptance criteria

- **Phase 1:** a running `exec` allocation's `memory.max` and `cpu.weight`
  change within one heartbeat of `alloc resize`, with no restart and no new
  job version; a session grow inside its envelope writes no raft entry; a
  grow that does not fit is refused with the node's free room.
- **Phase 2:** a sandbox's ceiling goes down and back up within its boot
  size with the guest running, on both VMMs.
- **Phase 3:** `job resize` changes every allocation of a task in place
  where it fits, replaces the rest, and `--reset` returns to the spec.
- **Phase 4:** `--move` resumes a `store` session at its new size on
  another node.
