# Judgement calls: sandbox boot throughput

> Plan: [`plans/sandbox-boot-throughput-plan.md`](../plans/sandbox-boot-throughput-plan.md).
> Entries are ranked by project impact, most impactful first. The number is
> the rank.

## 1. One stacked change, and most of the parallel branches stay out

**Call.** The work shipped as one branch, `perf/sandbox-scale`, holding this
plan's changes and three of the fifteen parallel `perf/*` branches:
`session-start-hot-path`, `rootfs-cow-cache`, and
`sandbox-start-critical-path`. The rest were left out: `sandbox-warm-pool`,
`edge-prewarm`, `elastic-session-envelopes`, `lazy-memory-restore`,
`chunked-checkpoints`, `checkpoints-on-stream`, `edge-admission-backpressure`,
`edge-ratify-throughput`, `multi-eval-plans`, `scheduler-lane-entry-cost`,
`flat-commit-cost`, `session-locality-routing` (empty), and
`web-fleet-throughput`.

**Alternatives.** (a) One PR per branch, as they were written. (b) Stack
every branch that compiled.

**Trade-off.** Work that may be good stays unmerged, as uncommitted diffs in
worktrees or a pushed branch (`perf/lazy-memory-restore`). Their ideas are
listed in `TODO.md` so they are not lost.

**Why (simplicity, then performance).** Mike asked for one PR without merge
conflicts, and for judgement over volume: keep the scheduler streamlined.
The bar was a gain measured on the rig, or a correctness or scale fix, worth
its code and surface. The three kept branches pass it: the router fixes
removed the hot-spot refusals measured under sustained load, the base disk
cache removed the per-session `mkfs`, and the stage timings are how every
later bottleneck here was found. The warm pool and pre-warm push (1,600 lines
each) are against the pre-warm stance until the cold path is measured good,
which it now is, at 180 to 230 ms. The checkpoint and keyed-lane branches do
not touch session boots. `lazy-memory-restore` needs cloud-hypervisor v52+,
which the rig did not run, so its gain was not measured.

## 2. A sandbox session is `running` once its guest takes exec

**Call.** A session launch waits, after the runtime's `start`, for
`TaskPlugin::wait_ready`. The sandbox runtime implements it as its guest
agent accepting `CONNECT 1100` on the VMM's vsock socket. Only then does the
claim reach `Serving`. The wait holds the launch slot, and gives up after 10
seconds with a warning, reporting the session running anyway.

```console
$ legion session start agents --task workspace --node https://10.0.1.7:4848 --ticket "$T" -j
{"ok":true,"data":{"sessionId":"workspace-q4n7c2rd.01jbx...","state":"running",...}}
# the guest takes exec now, about 140 ms after its VMM spawned
```

**Alternatives.** (a) Keep `running` as "the VMM process exists", and add a
`ready` state. (b) Wait inside the runtime's `start`. (c) A health check.

**Trade-off.** A launch slot is held through the boot, so
`edgeBoot.launchConcurrency` now bounds concurrent boots, not just spawns: a
16-vCPU node's 32 slots saturate at about 48 a second, which is also where
its CPU saturates. A guest whose agent never listens (an image the static
`legion-guest` cannot run on) costs its start 10 seconds.

**Why (simplicity, then performance).** "Running" that the caller cannot
use pushes a retry loop into every client, which is the opposite of easy.
(a) adds a state every reader has to learn. (b) runs under the task-manager
lock on the paths that start under it. Holding the slot bounds how many
guests boot at once on a node, which is what cut a burst's p99 from 2.3 s
to 1.0 s on four nodes at the same throughput.

## 3. cloud-hypervisor guests boot on virtio-console, Firecracker guests boot `quiet`

**Call.** CH gets `--serial off --console tty` and `console=hvc0`.
Firecracker, which has no virtio-console, keeps `ttyS0` and adds `quiet`. A
task's `kernel_args` come last, so `loglevel=7` restores a Firecracker
guest's full log.

**Alternatives.** (a) `quiet` everywhere. (b) Leave the console alone and
document the cost.

**Trade-off.** CH output from before the virtio-console driver registers is
held in the kernel's log buffer and replayed, so a panic that early prints
nothing. Firecracker boot logs are hidden by default. Chatty workloads still
pay the per-byte exit on Firecracker.

**Why (performance).** The serial console cost about 700 ms of a 4-vCPU
nested guest's boot. `hvc0` costs the same as `quiet` (about 120 ms to init
by hand) and keeps the log on CH, where most debugging happens. (a) would
throw it away for nothing.

## 4. Every cloud-hypervisor disk is declared `image_type=raw`

**Call.** The root disk, the volumes, and the identity disk all pass
`image_type=raw`.

**Alternatives.** (a) Ship root disks as qcow2. (b) A `rootwait`/retry in the
guest.

**Trade-off.** A checkpoint taken before the change restores with the old
config and can still hit the refusal. No installed base, so it is accepted
and stated in the rollout note.

**Why (performance, and correctness).** Without it CH probes the format and
refuses guest writes to sector 0 of a probed raw image. Ext4's mount writes
the superblock through block 0, so every boot failed its root mount,
panicked, and rebooted a second later. This alone took a boot from 2.8 s to
0.95 s. Every disk Legion attaches is raw, so declaring it costs nothing.

## 5. `stateful.placement` offers `spread` (default) and `fast`, and no more

**Call.** One field on session tasks. `spread` is the existing most-room
pick with in-flight reservations and random ties. `fast` is power of two
choices with a full-read fallback when its samples find no room.

```nickel
stateful = { mode = "session", envelope = { ... }, placement = "fast" },
```

**Alternatives.** (a) No knob: random ties make most-room good enough,
which the locality-routing investigation argued from a simulation. (b) A
`bin_pack` option. (c) Locality-aware routing.

**Trade-off.** A field users must choose, and `spread` keeps an O(nodes)
pick under the router's lock.

**Why (performance at scale won over simplicity).** Measured, a `spread`
pick is 31 µs on 1,000 nodes, 330 µs on 10,000, and 5.2 ms on 100,000,
capping a manager at about 3,000 and 190 starts a second. `fast` is under a
microsecond at every size. Mike asked for the choice between fastest and
best. `spread` stays the default because it balances better on the fleets
most clusters run. `bin_pack` (b) refused hundreds of starts per thousand
under stale views in the simulation. Locality (c) was judged
regime-specific.

## 6. A view that says every envelope is full asks the least full node once

**Call.** When the router's view finds no room, a first attempt still sends
the start to the node the view thinks least full. Only that node's refusal
refuses the create.

**Alternatives.** (a) Subtract a stop from the view when this manager
issues it. (b) Refuse on the view, as before.

**Trade-off.** A truly full fleet costs a create one wasted round trip.

**Why (performance, then simplicity).** The view is up to a second old and
sees stops only on refresh, so at about 86% utilization it called the fleet
full while nodes had room: 95 of 1,200 sustained starts were refused that
way. (a) only covers stops this manager made, not edge stops or another
manager's. Asking the authoritative node covers all of them in one place.

## 7. VMMs spawn with `posix_spawn`: a process group, and a shell that joins the cgroup

**Call.** `spawn_task_process` uses `process_group(0)` instead of a
`setsid()` hook, and `join_assignment_cgroup` rewrites the command into
`/bin/sh -c 'echo $$ > "$0" && exec "$@"' <cgroup.procs> <command...>`.
With no `pre_exec` hook left, the standard library spawns with
`CLONE_VM | CLONE_VFORK`.

**Alternatives.** (a) Keep the hooks. (b) A custom `clone3` spawner with
`CLONE_INTO_CGROUP`. (c) Write the pid into `cgroup.procs` after spawning.

**Trade-off.** Tasks are process-group leaders, not session leaders. Every
use in the code (signalling `-pid`, surviving the worker) needs only the
group. A cgroup join depends on `/bin/sh`, and costs one more exec.

**Why (performance, with the guarantee kept).** A `fork` copied the worker's
page tables per spawn, contending on its memory map with every other thread.
(b) is a hand-written spawner in unsafe code for one call site. (c) lets the
VMM run, and charge memory, outside its cgroup for a moment, which the
enforcement contract forbids. The shim keeps the contract: the VMM's first
instruction runs inside its cgroup.

## 8. Registry clients are shared per credential

**Call.** The OCI fetcher keeps one client per protocol, platform selection,
and credential fingerprint (up to 64), driven by one runtime.

**Alternatives.** (a) One shared client. (b) A client per fetch, as before.

**Trade-off.** Long-lived connections and a small cache of clients per node.

**Why (performance, with security).** A client per fetch re-parsed the
host's CA bundle and paid a token request and a `HEAD`, each behind a new
TLS handshake, on every start: 80 to 130 ms of every start by tag. (a) would
let one job's bearer token, which the client caches by repository rather
than by caller, serve another job's pull.

## 9. A tag is resolved once per burst, and an image already on the node needs no claim

**Call.** Tag resolution is single-flight per key: a pull uses an answer
sent after it arrived, finished or in flight, and otherwise sends (or waits
for) a newer one. The per-image pull claim is taken only when a manifest or
blob is missing from the node's content store.

**Alternatives.** (a) Keep the claim around the whole layout, which already
shared resolutions among queued pulls. (b) A short TTL on tag resolutions.

**Trade-off.** A small flight table beside the resolution cache.

**Why (performance).** Under the claim, a burst of starts of one image
assembled their layouts one at a time: the thousandth start of a burst on a
96-vCPU node waited over 5 s in `image`. (b) changes what a tag means: a
start could run a digest older than the registry's answer at the time it
asked.

## 10. A session's stop sends the guest's stop request off the lock, and writes no state file

**Call.** The sandbox's guest stop request runs on a pooled thread. A
request the guest cannot take kills the VMM from there. An ended unratified
claim never rewrites the worker's state file, on its stop or its teardown.

**Alternatives.** (a) Make the whole stop path asynchronous. (b) Coalesce
state-file writes.

**Trade-off.** A failed stop request kills the VMM without the flush the
hard-stop path tries, which a guest that cannot take a one-line exec could
not have done anyway.

**Why (performance, then simplicity).** Each session stop held the global
lock through an exec round trip into the guest and two full state writes
with fsync. On a 16-vCPU node that capped churn at about 26 a second, with
starts waiting hundreds of milliseconds to register. Sessions are never in
the state file, so writing it for them bought nothing. (a) is a rewrite of
the stop machinery for the same effect.

## 11. Launches and stop requests run on the runtime's blocking pool

**Call.** `admit_and_launch` and the stop request use `spawn_blocking` when a
runtime is present, a plain thread otherwise.

**Alternatives.** A dedicated pool sized by `launchConcurrency`.

**Trade-off.** Sessions share the runtime's blocking pool (512 threads) with
the worker's other blocking work. The launch pool bounds them.

**Why (simplicity).** A new thread per start maps and unmaps a stack under
the worker's memory-map lock. The runtime already keeps a pool. On metal
this did not raise the ceiling (entry 13), but it removes a per-start cost
with no new machinery.

## 12. A process's start time is read from `/proc/<pid>/stat`

**Call.** On Linux, `process_start_time` parses `/proc/<pid>/stat` and the
boot time, with `sysinfo`'s formula, so recorded values compare equal.

**Alternatives.** A newer `sysinfo`, or caching start times.

**Trade-off.** A Linux-only parser beside the `sysinfo` path other platforms
keep.

**Why (performance).** `sysinfo` asked for one pid still listed every
process and thread in `/proc`. With a few hundred VMMs on a node it was the
worker's largest CPU cost, and it ran per start and per stopping allocation
on every state write. Removing it raised a 4-node cluster's sustained
ceiling from about 46 to 56 boots a second.

## 13. The per-node ceiling on large nodes is accepted for now

**Call.** Ship with a 96-vCPU node booting about 50 a second while its CPUs
are about 90% idle, stated as a limit in the public doc.

**Alternatives.** Keep profiling on metal until it moves.

**Trade-off.** Large nodes do not turn their cores into boots. The public
doc tells operators to add nodes rather than cores.

**Why.** The budget ($10 for the campaign) was close to spent, and release
binaries without frame pointers could not attribute the remaining waits.
Evidence so far: `register` (the task-manager lock) up to 1.8 s and
`vmm_spawn` up to 2.6 s under a 1,000-start burst, with kernel samples in
page-table copying and memory-map lock contention. The next step is a metal
run with a frame-pointer build and lock hold-time instrumentation
(`TODO.md`).

## 14. Guests are timed from the host, by their agent's vsock listener

**Call.** The rig counts a guest up when a host connection to its vsock
socket gets `OK` for `CONNECT 1100`, stamped on the host's clock.

**Alternatives.** (a) The guest's own uptime at the command's start. (b)
When the console log file shows the command's line.

**Trade-off.** It measures "ready for exec", which comes slightly after the
command starts.

**Why (honest numbers).** (a) undercounted badly: a guest's uptime restarts
on the panic-and-reboot and misses VMM-side time, which first made the
baseline look like 1 s instead of 2.9 s. (b) measured the worker's log sink,
which flushes late under load. The vsock probe is what a caller waiting to
exec experiences.

## 15. The session index's heartbeat costs O(stops), not O(sessions)

**Call.** `ingest` checks pending duplicate stops against the node's own
map, instead of cloning every session id of the node into a set on every
heartbeat.

**Alternatives.** Only build the set when the node has stops.

**Trade-off.** None.

**Why (performance at scale).** Every heartbeat of every node takes the
index's write lock. Measured after the change, a heartbeat costs about 8 µs
at a million sessions.
