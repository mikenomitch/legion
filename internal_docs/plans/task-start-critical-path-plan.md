# Task Start Critical Path on the Node

> **Judgement calls:** [task-start-critical-path-plan_calls.md](../calls/task-start-critical-path-plan_calls.md)

**Status: IN EXECUTION.** Phases 1 to 3 are built. Phase 4 (start reports
that do not wait for a heartbeat) and phase 5 (the gated datapath and its
policy for a streamed placement before boot) are deferred and tracked in
[`TODO.md`](../TODO.md). Neither is measured on a real VMM yet.

## Problem

Once work reaches a node, the worker runs a chain of steps before the workload
serves: artifact staging, volume mounts, `prepare`, the runtime's `start` (for a
sandbox: a TAP and address lease, the image layout, the root disk, the VMM
config, the VMM spawn), registration under the task-manager lock, the guest's
boot, the first passing health check, and the report to the manager. Two things
were wrong with that chain.

- **Nothing on it was measured.** No metric or log said which step a slow
  start spent its time in. An operator with slow sandbox starts could not tell
  a root-disk build from a lock wait from a slow health check.
- **The health check, not the workload, set the pace.** A check runs once at
  start (`tokio::time::interval`'s first tick), when the workload is a few
  milliseconds old and fails, and then every `every` (10s by default). A
  sandbox whose guest serves 300 ms after the VMM spawns reported healthy ten
  seconds later. Rollouts and service discovery wait on that report.

A third, smaller one: on the heartbeat path the worker bound a gated
container's egress proxy, resolver, and upstream listeners *inside* its network
namespace before it created the namespace (`apply_upstreams` ran before
`apply_net_capture` in `heartbeat_tick`). The bind failed, the task started
with no network, and the next beat both reported it `net_capture_unavailable`
and bound the listeners.

## Prior art

- **Kubernetes** has a `startupProbe` with its own `periodSeconds` and
  `failureThreshold`, separate from liveness and readiness. A slow starter
  gets a long window, but a fast one still waits a full probe period: the
  kubelet does not probe faster than the period. Kubelet records per-pod
  startup latency (`kubelet_pod_start_sli_duration_seconds`) and
  per-operation runtime latencies, but not per-stage times for a single pod.
- **Nomad** checks run on `interval` from the start, with
  `check_restart.grace` to ignore early failures. No faster first probes.
- **Firecracker** ships a boot-timer device (`--boot-timer`) that logs the
  guest's boot time when init writes a magic value, because boot latency is
  what microVM platforms tune. It measures the guest only, not the host steps
  before the spawn.

We take the stage histogram (one family, bounded labels) and a startup
probe. We reject a new jobspec block for it: probing only reports passes
earlier, never counts a failure, so it needs no knob.

## Jobs to be done

**Cluster operator**
- When sandbox starts are slow on some nodes, I want to see which step of the
  start takes the time, so I can fix the right thing (disk, image cache, lock,
  boot) instead of guessing.

**Job author**
- When my service is up half a second after it starts, I want it to receive
  traffic then, so a deploy or a scale-up is not ten seconds slower than my
  service is.

**On-call responder**
- When one sandbox took a minute to start, I want one log line that breaks the
  start down, so I can say what happened without a profiler.

*Not in scope:* the autonomous agent persona has nothing new to call. It sees
faster `healthy` transitions through the surfaces it already reads.

## Goals and non-goals

Goals: every start's stages are observable; health is reported as soon as the
workload passes its check; no failure semantics change.

Non-goals: the root-disk build and image cache (the sandbox root-disk cache
work owns it), warm pools, lazy restore, edge admission, the session
control-plane path, and the task-manager lock held across a staged session
boot (owned by the session start hot-path work). The stage metric shows all of
them, which is the point.

## What exists today

- `src/worker/task_manager.rs`: `UnlockedLaunch::run` (mount, consume,
  `prepare`, `start`, off the lock), `finish_unlocked_launch` (register, state
  file), `prepare_registered_allocation` / `start_registered_allocation` (the
  locked path: provisional starts, restores, gated starts), the staging threads.
- `src/task_plugins/sandbox.rs`: `start_microvm` builds the TAP and lease, the
  layout, the root disk, and the VMM command. `start` spawns it.
- `src/worker/task_health.rs`: one tokio task per check on
  `tokio::time::interval(every)`, results to one monitor loop.
- `src/worker/daemon.rs`: `heartbeat_tick` binds the datapath
  (`apply_upstreams`), then later creates gated namespaces
  (`apply_net_capture`).

## Design

### 1. Stage timings

`legion_task_start_stage_seconds{runtime,stage}` on the worker registry.
Stages: `staging`, `volumes`, `prepare`, `start` (or `restore`), `register`,
`total`, `health`, and the sandbox's sub-stages of `start`: `network`, `image`,
`root_disk`, `vmm_config`, `vmm_spawn`. The timings travel on `TaskInstance`
(`start_timings`), so the runtime records its own sub-stages into the same
breakdown, and the worker logs one line per start:

```text
INFO Task allocation started allocation_id=4312 task_name=box task_runtime=sandbox
  total_ms=2611 stages="staging=1204ms prepare=1ms image=2ms root_disk=1380ms
  vmm_config=1ms vmm_spawn=3ms start=1389ms register=2ms"
```

### 2. Startup probing

Until a `tcp` or `http` boot or health check first passes after a start or
restart, the worker also runs it between `every` ticks: 100 ms after the start,
then doubling, capped at 1 s (or `every`). A probe uses at most a 1 s timeout.
Only a pass is reported. Failures are counted on the cadence alone, exactly as
before. `exec` checks and readiness checks keep their cadence.

### 3. Namespaces before the listeners inside them

`heartbeat_tick` creates gated namespaces before it binds the datapath, so a
gated container's proxy, resolver, and upstream listeners bind on the beat
that first lists it.

## Phases

1. Stage timings and the start log line. Ships alone: pure observability.
2. Startup probing. Ships alone.
3. Namespace ordering. Ships alone.
4. *(Deferred)* Start reports that do not wait for a heartbeat. A health
   transition reaches the manager on the next beat: about 1 s on average at
   the default 2 s cadence, and up to the adaptive cadence (21 s on a large
   fleet). Waking the heartbeat per transition would add a full heartbeat per
   start to a manager whose cadence exists to bound heartbeat load. The fix is
   a worker-to-manager report frame on `StreamWork`, which the checkpoint
   reports are moving onto.
5. *(Deferred)* The gated datapath for a streamed placement before boot. A
   placement from `StreamWork` starts at once, but its listeners, network
   policy, upstream endpoints, and credentials all arrive on the next beat.
   Binding the listeners early is not enough, because the policy is what lets
   anything through. The fix is to carry the policy assignment on the delta.

## Testing

- Unit: `start_timing` records, summarizes, bounds its list, and reaches the
  worker registry.
- Integration (`task_manager` tests): a TCP check with `every = 30s` turns
  healthy within a second of its port opening at 1.5 s, and the probes that
  failed before it (past the boot grace, `unhealthy_after_count = 1`) are not
  counted.
- Phase 3 needs Linux network namespaces. It is compile-checked, not
  exercised, on macOS.

## Rollout

Default on, no knob. A cluster sees health reported earlier and the same
failures. The metric is additive.

## Acceptance

- Phase 1: a sandbox start on a KVM node shows every stage in the histogram
  and the log line.
- Phase 2: start-to-healthy for a checked task tracks the workload's own
  start, not `every`.
- Phase 3: a gated container's first beat reports `capture_ready`.
