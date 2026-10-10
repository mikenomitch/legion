# Plan: Suspend, Resume, Fork, and Oversubscription

> **Judgement calls:** [`calls/suspend-resume-and-oversubscription-plan_calls.md`](../calls/suspend-resume-and-oversubscription-plan_calls.md)

Status: **IN EXECUTION** (written 2026-10-09, execution started 2026-10-10). Phases 1-7 shipped (3a content store, 3b runtime checkpoints, 3c snapshots API and restore, 4 suspend and resume, 5 automatic suspend, 6 fork, create from a snapshot, and named snapshots, 7a content publishing, the location index, and the `locality` score, 7b `durability = "store"`, cross-node resume, and `snapshot warm`). The integration of Phases 5, 6, and 7b finished their seams: `--spread` forks across nodes from a published checkpoint, published snapshots as named seeds, a bounded publish retry, and placement that binds each restore to its planned instance on a worker that can fetch. Resume guards shipped after Phase 7 (Track 2): `suspend.reserve` as a ratio or fixed amounts, the worker's `suspendedReserve` floor, make-room suspends for a resume pinned to a full node, and the checkpoint disk budget as a placement constraint ([Capacity for resumes](../../docs/suspend-and-resume.md#capacity-for-resumes), calls entries 5, 6, 10, 34, 43, 53, 101, 103, 120). Phase 8 (fast paths: wake on connect, the `hint` idle signal, lazy memory restore, diff snapshots) is deferred. The Linux and KVM checks for Phases 1-7 are pending ([verification plan](../suspend-resume-linux-verification.md)).

**Relationship to existing plans.** This plan supersedes the *phasing and
scope* of
[`sandbox-snapshots-and-fast-start-plan.md`](sandbox-snapshots-and-fast-start-plan.md)
and keeps its *mechanics*. The capture sequence, the manifest and `compat`
block, quiesce hooks, the identity-collision analysis, and the UFFD design in
that plan remain the reference, and this plan cites them by section instead of
restating them. What changes: snapshots stop being a sandbox feature and
become one of several generic primitives, suspend and idle policy move inside
Legion (that plan put warm pools and idle handling "outside"), and the blob
transport is specified here because the artifact cache it leaned on has not
shipped. This plan also *executes* Phases 1 and 3 of
[`artifact-cache-and-prewarm-plan.md`](artifact-cache-and-prewarm-plan.md),
with two amendments (§6.8).

Deliberately deferred and named: CRIU checkpoints for `exec` and `container`,
diff snapshots, usage-aware scheduling, wake-on-connect, lazy memory (UFFD),
and suspend for slot-mode instances. Each is in §4 (non-goals) or Phase 8.

---

## The plan in one page

Agent platforms want thousands of sandboxes per cluster, most of them idle at
any moment, each one able to vanish to disk and come back in under a second,
sometimes on a different machine, sometimes as five copies. Google's
[Agent Substrate](https://cloud.google.com/blog/products/containers-kubernetes/agent-substrate-available-on-gke)
and [AX](https://github.com/google/ax) build that as a dedicated actor runtime
on top of Kubernetes. We want the same outcome from **seven general
primitives** that each stand on their own and also serve ordinary services:

| # | Primitive | What it gives you alone | Section |
| --- | --- | --- | --- |
| 1 | **Usage and enforcement** for every runtime | Real memory/CPU per allocation, governed sandboxes | §6.2 |
| 2 | **Burstable reservations** (`memoryMax`) and **node oversubscription** | Pack more on a node, Nomad-style | §6.3 |
| 3 | **Memory pressure response** on the worker (named steps, previewable order) | Overcommit that degrades predictably instead of OOM-roulette | §6.4 |
| 4 | **Checkpoints** (memory or disk) as a runtime capability | Snapshot and roll back a workspace | §6.5 |
| 5 | **Suspended instances** that hold no resources | Park a sandbox, resume it later | §6.6 |
| 6 | **Idle policy** | Automatic suspend when nothing is happening | §6.7 |
| 7 | **Content locality** (node store, location index, `locality` scoring) | Cached artifacts and images, place work near its bytes | §6.8 |

Two features are **compositions**, with almost no new machinery of their own:

- **Fork** = checkpoint (4) + create N instances from it (5) + per-fork
  identity rebind, placed near the bytes (7). §6.9.
- **Wake on connect** = suspend (5) + a parked listener on the node.
  Sketched in §6.10, deferred to Phase 8.

And the agent use case is a jobspec, with no agent-specific subsystem (§6.11):

```nickel
{
    name = "workspace",
    runtime = "sandbox",
    stateful = { mode = "keyed", storage = { path = "/workspace", mode = "block" } },
    resources = { cpu = 1000, memory = 512, memoryMax = 4096 },
    suspend = {
        checkpoint = "memory",
        idle = { after = "3m" },
        onPressure = true,
    },
}
```

On a worker with `resourceEnforcement.oversubscribe = { memory = 2.0 }`, that fleet packs at
512 MiB per *running* sandbox against a doubled memory budget, bursts to
4 GiB when there is room, suspends to disk after three quiet minutes or
sooner when the node is under pressure, and resumes where it left off.

Eight phases, each shippable alone (§8). Phases 1–2 are useful to every
cluster. Phases 3–5 are the core of the agent story. Phases 6 and 7 are
independent of each other.

---

## 1. Problem statement

Concretely, today:

- **Every sandbox start is a cold boot.** The `sandbox` runtime builds a root
  disk, boots a kernel, runs `/legion/init`, and runs the command
  (`src/task_plugins/sandbox.rs:598-691`, `src/task_plugins/microvm.rs:482-561`).
  There is no pause, snapshot, or restore anywhere in the runtime or the
  `TaskPlugin` trait (`src/task_plugins/mod.rs:442-738`). An agent workspace
  that stops loses its warm language server, its REPL, and its loaded model.
- **An idle sandbox costs as much as a busy one.** A keyed instance is either
  `active` (holding its full reservation) or `stopped` (holding nothing, and
  cold-booting on `start`) (`src/structs/instance.rs:67-86`,
  `src/storage/store.rs:14599-14738`). Nothing detects idleness. The only
  way to reclaim memory from a sandbox that is waiting on a human is to stop
  it and pay a cold start later.
- **Reservations are the only memory model.** Resources are a single number
  per dimension with no request/limit split. The fit check is
  `available >= amount` over declared values (`src/resources.rs:678-689`),
  and the public docs say it plainly: "No oversubscription and no
  node-pressure eviction" (`docs/preemption-and-eviction.md:351-353`). A
  service that idles at 300 MiB and spikes to 2 GiB must reserve 2 GiB
  forever.
- **Nobody measures actual usage.** The worker fingerprints memory once at
  startup (`src/worker/daemon.rs:268`), and the heartbeat carries no usage
  fields (`proto/legion.proto:2861-2930`). The scheduler cannot tell a node
  at 95% real memory from one at 5%.
- **Sandboxes are not governed at all.** `sandbox` returns
  `ResourceEnforcement::Unsupported` (`src/task_plugins/mod.rs:581`,
  `docs/task-runtime-sandbox.md:307`), so a guest's memory is whatever the
  VMM allocates, outside any cgroup.
- **Nothing moves bytes to where work lands.** There is no node cache, no
  advertisement of what a node holds, and no locality scoring
  (`internal_docs/TODO.md:1304-1316`). Every allocation re-fetches its
  artifacts into a per-allocation staging dir
  (`src/artifacts/stage.rs:443-447`). A multi-GiB memory snapshot needs
  this badly.
- **Forking is impossible.** There is no way to say "give me five copies of
  this sandbox as it is right now."

What a platform team does instead today: build all of this outside Legion.
They keep a pool of running sandboxes, stop and cold-start them on their own
idle timers, over-provision memory, and accept a multi-second start on every
wake. That is the gap AX and Agent Substrate are being adopted to fill on
Kubernetes.

---

## 2. Prior art

### 2.1 Fly.io Machines: suspend as a machine state

[Fly Machines](https://fly.io/docs/reference/suspend-resume/) support
`suspend` alongside `stop`. Suspend pauses the Firecracker VM and writes a full
snapshot (memory, CPU registers, devices) to the machine's host. Resume takes
hundreds of milliseconds instead of a multi-second boot. Fly's proxy
[autosuspends](https://community.fly.io/t/autosuspend-is-here-machine-suspension-is-enabled-everywhere/20942)
idle machines and wakes them on the next request. Constraints are honest: at
most 2 GB of memory, no swap, no GPU, and the snapshot lives on the host, so
a suspended machine is pinned to it.

**Take:** suspend as a peer of stop. Idle-triggered suspend. Host-local
snapshots as the default. Disclosed size limits.
**Reject:** wake on request through a global proxy, for now. Legion has no
cluster edge (`docs/network-handlers.md:672`). Wake on connect is
Phase 8.

### 2.2 E2B: pause/resume sandboxes on Nomad

[E2B's self-hosted infra](https://github.com/e2b-dev/infra) runs Firecracker
sandboxes under Nomad and Consul, with pause/resume implemented as its own
layer above the scheduler. The scheduler has no notion of a paused sandbox,
so the platform reimplements placement for resume.

**Take:** confirmation that the market wants pause/resume for sandboxes.
**Reject:** putting it above the scheduler. If the scheduler doesn't know a
suspended instance exists, it cannot count it, place its resume, or prefer
the node that holds its bytes.

### 2.3 Google Agent Substrate and AX

[Agent Substrate](https://docs.cloud.google.com/kubernetes-engine/ai-ml/about-agent-substrate)
splits **actors** (agent instances) from **workers** (pods). A lightweight
control plane (`ateapi`) multiplexes actors onto a warm worker pool,
snapshots an idle actor's memory and files, and releases the worker. Google
claims under 500 ms resume and over 500 activations per second.
[AX](https://www.infoq.com/news/2026/09/google-ax-orchestrator/) layers a
`Task`/`Workspace` API on top and suspends agents during model and tool
calls.

**Take:** idle memory is the cost to attack. Snapshot-on-idle plus
release-the-capacity is the right shape, and resume latency is the headline
metric.
**Reject:** a second control plane and a separate actor/worker object model.
Substrate needs it because the Kubernetes API server cannot schedule at
actor rates. Legion's scheduler is the control plane, and an instance already
is the actor (stable id, storage handle, at most one live allocation). The
cost of Substrate's design is three stacked systems plus Redis. Practitioners
call that out (InfoQ, above), and it is the opening for Legion.

### 2.4 Kubernetes: agent-sandbox, checkpointing, QoS, swap

- [`kubernetes-sigs/agent-sandbox`](https://agent-sandbox.sigs.k8s.io/docs/)
  adds `Sandbox`, `SandboxWarmPool`, and `SandboxClaim`. A claim adopts a
  pre-warmed sandbox in about a second. **Take:** warm pools are a real need.
  **Reject:** a pool object. Resume from a local checkpoint makes the pool
  unnecessary for most uses (the old plan's §Part II.5 stance, kept).
- [Forensic container checkpointing (KEP-2008)](https://kubernetes.io/blog/2022/12/05/forensic-container-checkpointing-alpha/)
  is beta, but restore never got a first-class API, and the implicit restore
  path is being disabled by default after CVEs where restored processes kept
  the source container's credentials and seccomp state. Pod-level
  checkpoint/restore ([KEP-5823](https://www.kubernetes.dev/resources/keps/5823/))
  is still in progress. **Take:** this is the strongest evidence that CRIU
  checkpointing of arbitrary containers is a security and reliability
  minefield. VM snapshots have a much smaller, well-defined state surface.
  **Reject:** CRIU in this plan (§4).
- [QoS classes](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/)
  derive Guaranteed/Burstable/BestEffort from requests vs limits, and the
  kubelet evicts under node pressure in roughly that order.
  **Take:** request vs limit is the right model, and node-pressure eviction
  needs an order. **Reject:** named QoS classes as a concept. Legion already
  has priority bands (`src/structs/band.rs:60-160`), and "is this allocation
  above its reservation right now" is a measured fact, not a class.

### 2.5 Nomad: `memory_max`

Nomad's [`resources.memory_max`](https://developer.hashicorp.com/nomad/docs/job-specification/resources)
turns `memory` into the scheduled reservation and `memory_max` into the hard
limit, opt-in via the scheduler's memory-oversubscription setting. Nomad has
no node-pressure eviction, so oversubscribed nodes fall back to the kernel
OOM killer.

**Take:** the field shape, almost verbatim (`memoryMax`), and the fact that
it needs no new scheduler concept. **Reject:** stopping there. Without a
pressure response, oversubscription is a gamble on which task the kernel
kills.

### 2.6 Borg: reclamation

[Borg](https://research.google/pubs/large-scale-cluster-management-at-google-with-borg/)
schedules batch work into *reclaimed* resources (limits minus measured usage
of production tasks) and kills it when production needs the capacity back.
**Take:** measured usage is the input that makes aggressive overcommit safe.
**Defer:** usage-aware scheduling. Phase 1 collects the data. Using it in
placement is a later plan, once we have numbers from real fleets.

### 2.7 Firecracker, cloud-hypervisor, CodeSandbox

- Firecracker [snapshots](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md)
  (full and diff, file or UFFD memory backends), the
  [balloon device](https://github.com/firecracker-microvm/firecracker/blob/main/docs/ballooning.md)
  (inflate/deflate, stats, `deflate_on_oom`, free page hinting and reporting
  in recent releases), and
  [VMGenID](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/random-for-clones.md)
  for reseeding the guest RNG on clone. Restoring the same memory file with
  `MAP_PRIVATE` in several VMs gives copy-on-write sharing for free.
- cloud-hypervisor exposes `pause`/`snapshot`/`restore` on the API socket
  Legion already creates (`src/task_plugins/microvm.rs:357`) and supports a
  balloon with free page reporting.
- CodeSandbox's [VM cloning writeup](https://codesandbox.io/blog/how-we-clone-a-running-vm-in-2-seconds)
  shows live fork in production: pause, snapshot, copy-on-write memory and
  disk, resume both sides, and fix up identity in the guest.

### 2.8 Distilled requirements

1. Suspend must be a scheduler-visible instance state that releases capacity.
2. Resume must prefer the node that already holds the bytes, and must be able
   to land elsewhere when the bytes have been published.
3. Overcommit needs a reservation/limit split, measured usage, and a defined
   order of degradation before the OOM killer.
4. Memory snapshots are RAM dumps. They need their own ACL and must refresh
   identity on resume.
5. Every primitive should work for `exec` and `container` too, at reduced
   fidelity (disk checkpoints), so it is a cluster feature and not a sandbox
   feature.

---

## 3. Jobs to be done, by persona

**Agent platform builder** (the job author for a fleet of sandboxes)
- When a user stops typing for a few minutes, I want their sandbox to stop
  costing me memory without losing what's running inside, so I can pack ten
  times as many users onto the same machines.
- When the user comes back, I want their sandbox exactly as they left it in
  well under a second, so it feels like it never went away.
- When an agent wants to try three approaches to a task, I want three exact
  copies of its current environment, so each attempt starts from the same
  point and the losers can be thrown away.
- When I build a "golden" environment with dependencies installed and caches
  warm, I want new sandboxes to start from it instead of from scratch, so the
  first command a user runs is fast.

**App developer** (ordinary services, no sandboxes)
- When my service usually needs 300 MB but spikes to 2 GB, I want to reserve
  for the usual case and still survive the spike, so I'm not paying for
  memory I use one hour a day.
- When my service gets killed for memory, I want to know whether it was my
  own limit or the machine being full, so I can fix the right thing.

**Cluster operator**
- When I see that nodes are reserved at 90% but actually use 30%, I want to
  let the scheduler pack them harder on the pools where that's safe, so I
  can stop buying machines I don't need.
- When a packed node runs short of memory, I want the least important and
  most idle work to give way first, in an order I can predict, so a memory
  spike doesn't take out a database.
- When a machine dies, I want to know which parked sandboxes died with it,
  so I can tell users the truth.

**Security / platform admin**
- When someone can snapshot a sandbox's memory, I want that to be a separate
  permission from deploying it, so a deployer can't quietly dump another
  tenant's secrets.
- When a sandbox resumes or is copied, I want it to get fresh credentials,
  so a copy can't impersonate the original.

**Autonomous agent** (driving Legion over MCP or the CLI)
- When I'm finished with a workspace for now, I want to park it and get a
  clear yes/no back, so I can resume it later by name without guessing.
- When I fork my environment, I want the new names back in one structured
  answer and a safe way to retry, so a network blip doesn't leave me with
  ten copies.

**On-call responder**
- When a sandbox fails to resume, I want to see why and get it running again
  from scratch if I have to, so one bad snapshot doesn't lock a user out.

### User scenarios

**S1. Park and resume.** An agent platform runs 2,000 keyed sandbox
instances on 20 nodes. At any moment about 150 are in use. Each idles to
`suspended` three minutes after its last network or CPU activity. Its memory
lands in a file on the node, and its 4 GiB guest stops costing anything. A
user returns, the platform calls `StartInstance`, and the sandbox resumes on
the same node in about 200 ms with the shell history and the dev server
intact.

**S2. A bursty API under oversubscription.** An app developer sets
`memory = 400, memoryMax = 2048` on a service. The operator has set
`oversubscribe = { memory = 1.5 }` on the `general` pool. The scheduler
packs on 400 MiB. A traffic spike pushes three replicas on one node past
their reservations at once. The node crosses its pressure threshold, the
worker evicts a `low`-band batch job that is over its own reservation, and
the service rides out the spike. The batch job reschedules with
`cause = node_pressure`.

**S3. Fork to explore.** An agent in sandbox `run-17` has a repo cloned, a
build cached, and a test server running. It calls
`legion instance fork run-17 --count 3`. The origin pauses for under a second
while its memory is captured, then keeps running. Three new instances come up
on the same node, sharing the snapshot's memory pages copy-on-write, each
with its own id, hostname, MAC, RNG seed, and workload identity token. The
agent keeps the winner and destroys the other two.

**S4. Golden image.** A platform builds `py-ml-base` once a week: boot, install
packages, import torch, then checkpoint. `snapshot warm py-ml-base` pushes it
to every node in `us-west`. `instance create --from-snapshot py-ml-base`
starts sandboxes that skip boot and imports entirely.

---

## 4. Goals and non-goals

### Goals

- **G1.** A keyed instance can be **suspended** (checkpoint, release
  resources) and **resumed** (place, restore) through the instance API, CLI,
  and MCP, with structured output and idempotency keys.
- **G2.** Memory checkpoints on `sandbox` (Firecracker and cloud-hypervisor),
  and disk checkpoints on every runtime with a stateful volume.
- **G3.** Idle-triggered suspend, with idle defined by signals the worker
  already sees or can cheaply measure.
- **G4.** `memoryMax` and per-node `oversubscription` ratios, governed by a
  worker-side pressure ladder with a documented order.
- **G5.** Real per-allocation memory and CPU usage reported to the manager,
  exposed in metrics, `instance list`, and `node usage`.
- **G6.** A node content store, a location index, and a `locality` scoring
  term shared by artifacts, images, and snapshots.
- **G7.** Fork: N new keyed instances from a live instance or a named
  snapshot, with per-fork identity.
- **G8.** Every primitive off by default, and a cluster that sets nothing new
  behaves exactly as it does today.

### Non-goals

- **CRIU checkpoints for `exec` and `container`.** Those runtimes get disk
  checkpoints (stop with storage retained) and therefore suspend, idle
  policy, and wake, but always cold-boot on resume. §2.4 is why.
- **Usage-aware placement.** The scheduler keeps packing on reservations
  (times the ratio). Phase 1's usage data feeds the worker's pressure ladder,
  metrics, and a future plan.
- **A warm-pool object.** Resume from a local checkpoint is the warm pool.
  Platforms that want pre-created instances use `instance create
  --from-snapshot` ahead of demand, as the old plan said.
- **A cluster edge or global request router.** Wake on connect (Phase 8) is
  node-local only.
- **Suspend for slot-mode instances and stateless tasks.** Slot counts and
  rollouts would need to learn what a suspended slot means. Keyed instances
  are where agents live. Open question 1.
- **Diff snapshots, lazy memory, cross-region replication.** As in the old
  plan, later or outside.
- **GPU sandboxes.** VMM snapshot support for passthrough devices does not
  exist in either VMM.
- **CPU pressure handling.** CPU is already work-conserving under
  `cpu.weight` (`src/worker/resources.rs:125-142`). The ladder is for memory,
  the resource whose exhaustion kills things.

---

## 5. Background: what exists today

**Instances.** Keyed and slot instances have a stable id, a storage handle,
and at most one live allocation
(`src/storage/migrations.rs:415-429`, `src/structs/instance.rs:23-147`).
`InstanceState` is `pending | active | rescheduling | stopped | terminated`.
`stop` deletes the allocation and keeps the row and handle in one
transaction (`src/storage/store.rs:14599-14668`). `start` moves
`stopped → pending` (`:14683-14738`). A keyed task's desired count is its
instances not in `stopped` or `terminated` (`:14745-14756`). Instance RPCs
gate on `job:write` (`src/manager/server.rs:4539-4571`). There are no
`instance.*` events.

**Allocations** have a free-string status (`src/constants.rs:34-50`). A stop
deletes the row, the worker sees it missing from
`HeartbeatReply.desired_allocations`, and `StopCause` records why
(`src/structs/stop.rs:27-78`).

**Sandbox runtime.** Backends are cloud-hypervisor, Firecracker, Apple
`container`, and an OCI dev mode (`src/task_plugins/microvm.rs:79-146`).
cloud-hypervisor launches with `--api-socket` that nothing connects to
(`:357`). Firecracker launches `--no-api --config-file` with no control
channel (`src/task_plugins/sandbox.rs:733-740`). Guest memory is fixed at boot
from `config.memory_mib` (default 512), separate from `resources.memory`
(`sandbox.rs:52-85`, `microvm.rs:351,404`). There is no balloon, hotplug, or
free page reporting. A guest agent exists, `legion guest-agent`, but it only
relays proxy and DNS ports over vsock for gated networking
(`src/cmd/guest_agent.rs:1-5`). The microVM path has no exec or actions
(`docs/exec-and-actions.md:133`), so the old plan's quiesce-hooks-via-actions
do not work on sandboxes yet.

**Task runtime trait.** `prepare/start/stop/destroy`, `stop_with`,
`signal`, `exec`, `supported_volume_modes`, `resource_enforcement`, network
gate hooks (`src/task_plugins/mod.rs:442-738`). No checkpoint hook.

**Volumes.** `VolumeOp` reserves `Snapshot`, `Restore`, `Clone`, `Expand`,
but the `VolumeDriver` trait dispatches only
`capabilities/mount/unmount/destroy` (`src/volumes/mod.rs:61-80,554-572`).
`local-dir` block mode is a sparse ext4 image (`src/volumes/builtins.rs:61-160`).

**Resources and fit.** `cpus`/`cpu` (MHz), `cores`, `memory` (MiB), custom
counters (`src/resources.rs:43-90,1081-1083`). Node capacity comes from the
registration fingerprint minus `reservedResources`
(`src/resources.rs:552-600`). Fit is `ResourcePool::can_fit`
(`:678-689`), re-checked in the apply transaction
(`src/storage/store.rs:5065-5074`).

**Enforcement.** Opt-in per worker (`resourceEnforcement.enabled`). Cgroup
v2, with `memory.max = memory`, `memory.swap.max = 0`, `cpu.weight` from MHz,
`cpuset.cpus` for `cores` (`src/worker/resources.rs:525-549`). `container`
is runtime-applied, `exec` uses a worker cgroup, `sandbox` is unsupported.
OOM kills are annotated from `memory.events`
(`src/worker/resources.rs:583-593`). Nothing reads `memory.current` or
`cpu.stat`.

**Preemption.** Bands `critical..background`, victims strictly lower band,
older than `victimExemptAge`, never node-pinned storage
(`src/manager/scheduler/preemption.rs:272-385`). No node-pressure eviction.

**Scoring.** A fixed weighted sum of `affinity`, `spread`, `bin_pack`,
`automatic_anti_affinity` (`src/manager/scheduler/policy.rs:19-49`),
computed in `score_node_for_context` (`policy.rs:277-301`) and in the
cached-batch path (`src/manager/scheduler.rs:4414-4449`). Weights are set
under `schedulerSettings.weights` (`scheduler.rs:3903-3950`).

**Artifacts.** Fetch-only (`ArtifactFetcher` has no put,
`src/artifacts/fetcher.rs:202-221`). No node cache, no advertisement, no
pre-warm (`internal_docs/TODO.md:1304-1316`). `ArtifactSink` is documented as
the seam where a content-addressed store writer goes
(`src/artifacts/stage.rs:101-124`).

**Heartbeat.** Allocation ids, failures, health, volume reports, terminated,
provisional claims, net stats (`proto/legion.proto:2861-2930`). The worker
already counts per-allocation network bytes (`src/worker/netd/mod.rs:54`).

**Workload identity.** ES256 JWT, default TTL 15m, bound to the allocation
id, refreshed at half TTL, delivered to sandboxes by env only
(`docs/workload-identity.md:26-41,138,186`).

---

## 6. Proposed design

### 6.1 How the primitives compose

```
                 ┌──────────────────────── manager ─────────────────────────┐
                 │ instance state machine   snapshot table   location index │
                 │ (active ⇄ suspended)     (refs to blobs)   (soft state)   │
                 │            │                    │                │       │
                 │            └──── scheduler: fit(reservation × ratio),     │
                 │                  compat constraints, locality score ──────┤
                 └───────────────▲──────────────────────────────────────────┘
          heartbeat: usage, idle_since, content deltas, local suspensions
                 ┌───────────────┴────────────── worker ────────────────────┐
                 │ cgroup per alloc ─► usage ─► idle detector ─► suspend     │
                 │        │                         ▲                         │
                 │        └──► pressure ladder ─────┘ (reclaim → suspend →   │
                 │                                    evict → OOM)           │
                 │ runtime.checkpoint/restore ─► node content store ─► publish│
                 └───────────────────────────────────────────────────────────┘
```

The manager owns **state, placement, and authorization**. The worker owns
**measurement, local reflexes, and bytes**. Each arrow is a primitive you can
use without the others.

### 6.2 Primitive 1: usage and enforcement everywhere

**Sandbox enforcement.** The VMM process for each sandbox allocation runs in
the allocation's worker cgroup, the same one `exec` uses. `memory.max` is the
guest size plus a fixed VMM overhead (default 64 MiB, configurable as
`sandbox.vmmOverheadMiB` on the worker), `cpu.weight` comes from the task's
`cpu`. `sandbox` starts returning `ResourceEnforcement::WorkerCgroup`. This
closes `TODO.md:1224-1233` and is a prerequisite for everything else, because
a VM you cannot measure cannot be suspended under pressure.

**Usage reporting.** Every enforced allocation reports, at a low cadence
(default every 10s, piggybacked on the heartbeat that falls due):

```
AllocationUsage {
  allocation_id,
  memory_current_bytes,      // memory.current
  memory_peak_bytes,         // memory.peak since start
  memory_anon_bytes,         // memory.stat anon (what a suspend would free)
  cpu_usage_usec,            // cpu.stat usage_usec, monotonic
  psi_memory_some_avg10,     // memory.pressure, per alloc
  last_activity_unix,        // max(net activity, cpu above floor, exec session)
}
```

The manager keeps usage in memory only, beside liveness, and never writes it
to the store (same reasoning as
[liveness off the raft path](liveness-off-the-raft-path-plan.md): high
churn, low value after a restart). It is served by `GetNodeUsage`, folded into
`InstanceInfo`, and exported as Prometheus gauges.

```console
$ legion node usage w-03
RESOURCE   CAPACITY      RESERVED          USED            PRESSURE
memory     96 GiB ×2.0   171 GiB (89%)     61 GiB (64%)    some 0.4%
cpu        64000 MHz     41200 MHz (64%)   18900 MHz (30%)

ALLOCATION        INSTANCE  RES/MAX MiB   USED MiB   IDLE
a-91c2 workspace  user-42   512/4096      1830       -
a-77e0 workspace  user-17   512/4096      402        2m41s
...
```

### 6.3 Primitive 2: burstable reservations and node oversubscription

**`memoryMax` on the task.** `memory` stays the reservation the scheduler
packs on. `memoryMax` is the ceiling the task may use when the node has
room. Unset means today's behavior exactly: the reservation is the limit.

```nickel
resources = {
    cpu = 500,          # MHz
    memory = 400,       # reserved: what the scheduler packs on
    memoryMax = 2048,   # ceiling: hard limit, enforced where the runtime can
},
```

Enforcement on cgroup runtimes sets `memory.max = memoryMax` and
`memory.low = memory`. `memory.low` makes the kernel reclaim from tasks above
their reservation before it touches tasks within it, which is the property
the pressure ladder relies on (§6.4). Validation rejects
`memoryMax < memory`. A runtime whose `resource_enforcement()` is
`Unsupported` accepts the field with a submit warning, matching how
`resources` already behaves there.

**Sandbox guest size follows `resources`.** Today a sandbox has two memory
numbers, `config.memory_mib` (guest size) and `resources.memory` (what the
scheduler counts), and nothing ties them together. This plan makes the guest
size `memoryMax` if set, else `memory`. An explicit `memory_mib` that
disagrees with the derived value is rejected at submit with the fix spelled
out. A sandbox with `memory = 512, memoryMax = 4096` boots a 4 GiB guest and
reserves 512 MiB. With free page reporting (Phase 5), the host gets back
pages the guest frees, so an idle-but-running guest's real footprint shrinks
toward its working set.

**Node oversubscription.** A worker may declare that its schedulable
capacity is a multiple of its physical capacity. The ratio lives *inside*
`resourceEnforcement`, because oversubscription without cgroups to act on is
a bet on the kernel OOM killer. Nesting it makes that misconfiguration
impossible to write rather than a warning to ignore.

```nickel
# configs/workers/default.ncl
resourceEnforcement = {
    enabled = true,
    # Schedulable memory = (physical − reservedResources.memory) × 1.5.
    # CPU is weight-shared, so `cpu` only changes how much the scheduler
    # is willing to place and never throttles anything. Default 1.0 (off).
    oversubscribe = { memory = 1.5, cpu = 1.0 },
},
```

The ratio rides registration into `ResourcePool::from_node`, so `can_fit`
and the apply-transaction recheck need no change in shape, only a multiplied
capacity. The policy fingerprint that keys the cached `NodeUsage`
(`src/resources.rs:101-129`) gains the ratio, so a ratio change invalidates
cached free pools instead of silently overcommitting further.

**The honest guarantee.** With `memoryMax` alone and ratio 1.0, the sum of
reservations still fits in physical memory, so a task within its reservation
is never evicted for pressure. Only tasks above their reservation compete.
A ratio above 1.0 **voids that guarantee**: reservations can exceed physical
memory, and the ladder may have to act on tasks within their reservation.
That is why the ratio is per node, off by default, and documented as "use
it where the workloads are idle-heavy or suspendable."

### 6.4 Primitive 3: memory pressure response

> **Revised at execution time** (see calls doc). The first draft exposed
> numbered "rungs", PSI field names (`psiSomeAvg10`), and a top-level
> `pressure` block that silently did nothing without enforcement. Users had
> to learn the kernel's vocabulary and a five-row table to answer one
> question: *what happens to my task when the node runs short?* The shape
> below answers that question directly.

**What a job author needs to know, in three sentences.** A task using no
more than its `memory` reservation is left alone (on nodes with no
oversubscription, always). A task *bursting* above its reservation gives
memory back first and is the first thing stopped if that isn't enough. A
task that sets `suspend.onPressure = true` is suspended with its state
intact instead of stopped.

**Operators set nothing to get it.** The response is on whenever
`resourceEnforcement.enabled` is, with defaults that scale with the node.
The block exists for tuning:

```nickel
resourceEnforcement = {
    enabled = true,
    oversubscribe = { memory = 1.5 },
    memoryPressure = {
        # Act when free memory (MemAvailable) drops below this.
        # Default: 5% of node memory, at least 256MiB, at most 4GiB.
        freeBelow = "2GiB",
        # ...or when tasks spend more than this share of time stalled
        # waiting on memory (kernel PSI "some", 10s average). Default 10%.
        stallAbove = "10%",
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

**The steps.** Named, not numbered, so an event or stop reason reads on its
own. Each step runs only until pressure clears. The kernel OOM killer stays
the backstop after the last step.

| Step | What it does | Who it can touch | Reversible |
| --- | --- | --- | --- |
| `reclaim` | `memory.reclaim` on cgroups above their reservation, inflate balloons on sandboxes above theirs | Bursting allocations | Yes, transparent |
| `suspend` | Checkpoint and suspend idle instances whose task sets `suspend.onPressure = true` | Opted-in instances, most idle first | Yes, resumes later |
| `evict-bursting` | Stop allocations above their reservation | Bursting allocations | Rescheduled, `cause = node_pressure` |
| `evict-reserved` | Stop allocations within their reservation. Only when `oversubscribe.memory > 1` | Anyone below `critical` | Rescheduled, `cause = node_pressure` |

Within a step, candidates sort by band ascending (the preemption order from
`preemption.rs`), then by bytes freed descending (`suspend` uses idle time
descending). Node-pinned storage is never evicted, matching preemption's
rule. It can still be suspended, which is the point: suspend keeps the
storage where it is.

**See the order before it matters.** The ordering is a pure function of
usage, bands, and job config, so the worker computes it continuously and
reports it with usage. `legion node usage` prints it, which turns "what
will happen" from a docs lookup into a command:

```console
$ legion node usage w-03
RESOURCE   CAPACITY      RESERVED          USED            PRESSURE
memory     96 GiB ×1.5   131 GiB (91%)     61 GiB (64%)    ok (free 35 GiB, stall 0.4%)

UNDER PRESSURE, IN ORDER
#  ALLOCATION        BAND     RES/USED MiB   STEP
1  a-91c2 workspace  normal   512/1830       reclaim, then suspend
2  a-31f0 batch      low      400/1900       reclaim, then evict-bursting
3  a-77e0 api        high     2048/1900      evict-reserved
```

Every action is reported in the heartbeat (`pressure_actions`) and becomes
a `node.pressure` event and a `StopCause`:

```console
$ legion alloc status a-31f0
Status:  stopped (node_pressure)
Detail:  evicted by evict-bursting on w-03: using 1.9 GiB of a 400 MiB
         reservation while free memory was 1.1 GiB (threshold 2 GiB)
Hint:    raise resources.memory toward real usage, or move this job to a
         pool without oversubscription
```

That message answers the app developer's job: "my own limit or the
machine?" An OOM at `memoryMax` keeps today's annotation (`oom_kill` from
`memory.events`), so the two are distinguishable.

### 6.5 Primitive 4: checkpoints

A **checkpoint** is a point-in-time capture of an allocation, recorded as a
**snapshot** row. The capture mechanics, manifest, `compat` block, and
quiesce hooks are exactly as in the old plan's
[Part I](sandbox-snapshots-and-fast-start-plan.md#part-i--snapshots).
This plan adds three things.

**The runtime capability.** `TaskPlugin` grows:

```rust
/// What this runtime can capture. Default: CheckpointSupport::Disk if the
/// task has a stateful volume whose driver advertises `snapshot`, else None.
fn checkpoint_support(&self, config: &Value) -> CheckpointSupport;
/// Pause, capture memory and device state into `out`, then either resume
/// (fork, manual snapshot) or leave the VMM stopped (suspend).
async fn checkpoint(&self, alloc, task, inst: &mut TaskInstance,
                    out: &CheckpointDir, then: AfterCheckpoint) -> R<MemoryParts>;
/// Start from a manifest instead of booting. Called in place of `start`.
async fn restore(&self, alloc, task, inst: &mut TaskInstance,
                 manifest: &SnapshotManifest, identity: &ResumeIdentity) -> R<()>;
```

`CheckpointSupport` is `None | Disk | Memory`. `sandbox` on Firecracker and
cloud-hypervisor returns `Memory`. Apple `container` and the OCI dev backend
return `Disk`. `exec` and `container` return `Disk` when the task has a
snapshot-capable volume, else `None`.

**A disk checkpoint without a disk snapshot.** For suspend, a disk
checkpoint on a node-local volume does not need the volume `snapshot` op at
all: the volume is retained in place, exactly like `stopped` today. The
`snapshot` op is needed only to keep a *copy* (manual snapshot, fork,
rollback). This lets `exec` and `container` suspend in Phase 4 before any
volume driver implements `snapshot`.

**Where the bytes go.** Memory parts are written into the **node content
store** (§6.8) by digest, not into the allocation's scratch dir, so they
survive the allocation and can be deduplicated, advertised, and published.
Until Phase 7 the store is local-only, which is enough for suspend and resume
on the same node.

**Firecracker launch change.** Firecracker gains `--api-sock` alongside
`--config-file`, only when the task's `checkpoint_support` is `Memory` and
the job enables `suspend` or snapshots. The old plan §3 has the API calls.
Every memory-checkpointable sandbox also reserves a ~1 MiB **identity disk**
at boot (old plan Part III), because a restored Firecracker VM can repoint an
existing drive but cannot hotplug a new one. Reserving it unconditionally for
those sandboxes is what makes fork possible later without a reboot.

**Restore compatibility** (old plan §5) becomes constraints derived from the
manifest at placement time, against new fingerprint attributes:
`sandbox.vmm`, `sandbox.vmm.version`, `cpu.vendor`, `cpu.model`, and a
`cpu.features` digest. Firecracker CPU templates can widen this later.

### 6.6 Primitive 5: suspended instances

`suspended` is a **new instance state**, a peer of `stopped`:

| | `stopped` | `suspended` |
| --- | --- | --- |
| Who puts it there | An operator or API call (`instance stop`) | `instance suspend`, idle policy, or the pressure ladder |
| Allocation | Deleted | Deleted |
| Resources reserved | None | None (default), see `suspend.reserve` |
| Storage | Retained | Retained |
| Checkpoint | None | `checkpoint_id` points at a snapshot |
| `start` does | Cold boot | Restore from checkpoint (cold boot with `--cold`) |
| Auto-wake (Phase 8) | Never | Yes, if configured |
| Counts toward desired | No | No |

The distinction matters because the system, not a human, puts instances
into `suspended`. An auto-wake must never wake an instance an operator
deliberately stopped, and `instance list --state suspended` is the number
an operator watches.

**Suspend flow.**

1. `SuspendInstance` (or the idle policy) checks ACLs, then CASes the
   instance `active → active` with `pending_op = suspend`, and puts the
   allocation on `HeartbeatReply.suspend_allocations`.
2. The worker runs quiesce hooks if configured, calls
   `runtime.checkpoint(.., AfterCheckpoint::Stop)`, writes the parts to the
   content store, stops the VMM, unmounts but **does not destroy** volumes,
   and reports `SuspendResult { allocation_id, manifest, bytes, duration }`.
3. The manager, in one transaction: inserts the snapshot row
   (`kind = suspend`, `location = node`), deletes the allocation row (which
   frees the reservation through the existing usage path), and sets the
   instance to `suspended` with `checkpoint_id`. It emits
   `instance.suspended`.
4. On checkpoint failure the instance stays `active`, the allocation keeps
   running, and the manager emits `instance.suspend_failed` with the
   runtime's error. Suspend never kills a workload it failed to capture.

Pressure-ladder suspends run steps 2 and 3 in the opposite order: the worker
acts first and reports in `local_suspensions`. The manager applies the same
transaction when the report arrives. A worker that suspends and then loses
contact keeps the checkpoint on disk, and the manager sees the allocation
terminate with a local-suspension record when it reconnects.

**Resume flow.** `StartInstance` on a suspended instance moves it to
`pending` with `restore_from = checkpoint_id`. The scheduler places it with:

- The compat constraints from the manifest (§6.5).
- **A hard pin** to the checkpoint's node while the checkpoint is
  `durability = "local"`, exactly like `node_local` volume pinning.
- The `locality` score (§6.8) once the checkpoint is published.

`TaskAllocation` carries the manifest. The worker calls `runtime.restore`
instead of `start`. The guest agent's resume hook (§6.9) refreshes identity.
A suspend-and-resume on the same node keeps the same node-local volume and
needs no data movement at all.

**When resume can't restore.** If restore fails (corrupt checkpoint, compat
drift after a VMM upgrade, checkpoint lost with its node), the behavior is
`suspend.onRestoreFailure`:

- `"cold"` (default): boot fresh on the retained storage, emit
  `instance.restore_failed` with the reason, and keep the instance usable.
- `"fail"`: leave the instance `suspended` with the error in `detail`, for
  platforms that would rather surface the loss to their user.

`legion instance start user-42 --cold` is the responder's escape hatch:
discard the checkpoint and boot.

**Holding the reservation.** `suspend.reserve = true` keeps the instance's
reservation on its node while suspended. Resume is then guaranteed capacity,
at the cost of the entire density win. It exists for the "must resume in
200 ms, always" tier. Default `false`.

> **Revised (Track 2).** `reserve` also takes a ratio or fixed amounts, a
> worker floor (`resourceEnforcement.suspendedReserve`) holds a share for
> every suspended instance on a node, and the manager suspends idle
> suspendable work to make room for a resume pinned to a full node. See the
> public doc's [Capacity for resumes](../../docs/suspend-and-resume.md#capacity-for-resumes).

**Capacity at resume.** When a suspended instance's node is full, resume is
an ordinary pending placement: it waits, and the instance's band can
preempt lower bands like any placement. A local-durability checkpoint cannot
move, so a full node means the resume waits (or, with
`onRestoreFailure = "cold"` and a movable volume, cold-boots elsewhere after
`suspend.resumeTimeout`, default 30s). This is the cost of local durability,
and Phase 7's `durability = "store"` is the fix.

**CLI and MCP.**

```console
$ legion instance suspend user-42 --wait -j
{"ok":true,"data":{"instance":"user-42","state":"suspended",
 "checkpoint":"snap-91c2","kind":"memory","bytes":1918893056,
 "duration_ms":740,"node":"w-03","durability":"local"}}

$ legion instance start user-42 -j
{"ok":true,"data":{"instance":"user-42","state":"active",
 "resumed_from":"snap-91c2","node":"w-03","restore_ms":212}}

$ legion instance list --job agents
ID        STATE      NODE   MEM RES/MAX/USED   IDLE    CHECKPOINT
user-42   active     w-03   512/4096/1830      -       -
user-17   suspended  -      -                  -       snap-77e0 (memory, local w-03)
user-09   suspended  -      -                  -       snap-1a4d (disk)
```

`suspend` takes `--checkpoint memory|disk` to override the job default and
`--idempotency-key`. MCP gains `legion_instance_suspend` (mutating) and
`InstanceInfo` grows the new fields, so `legion_instance_get` covers resume
diagnostics.

Errors are actionable at submit and at call time:

```console
$ legion instance suspend api-3 --checkpoint memory
Error: instance api-3 cannot take a memory checkpoint: runtime "container" supports "disk" only
Hint:  use --checkpoint disk, or run this task with runtime = "sandbox"
```

### 6.7 Primitive 6: idle policy

```nickel
suspend = {
    checkpoint = "memory",          # "memory" | "disk". Default: the best the runtime supports
    idle = {
        after = "3m",               # quiet for this long → suspend
        # What counts as activity. Default: ["network", "cpu", "exec"].
        #   network     : bytes through any Legion-owned listener or proxy
        #   connections : any open ingress connection, even a quiet one
        #   cpu         : usage above cpuBelow (fraction of the task's cpu)
        #   exec        : an open exec session
        #   hint        : the workload's own busy/idle signal (Phase 8)
        signals = ["network", "cpu", "exec"],
        cpuBelow = 0.02,
    },
    onPressure = true,              # the `suspend` pressure step may suspend this
    reserve = false,
    onRestoreFailure = "cold",      # "cold" | "fail"
    durability = "local",           # "local" | "store" (Phase 7)
},
```

**The worker detects, the manager decides.** The worker already sees every
signal (netd byte counters per allocation, cgroup CPU, exec sessions). It
computes `idle_since` per allocation and reports it with usage. The manager
runs a low-frequency idle sweep (every 15s) and issues suspends for instances
past `after`. Suspend is a state transition, so it belongs to the component
that owns the instance row. The worker's only autonomous suspend is the
pressure ladder, where waiting on the manager would be wrong.

**Why the signals are a list.** An agent harness waiting 40 seconds on a model
call has an open, mostly quiet HTTPS connection. Counting only
`connections` would never suspend it. Counting only `network` would suspend
it mid-call and break the stream on resume. The default (`network` +
`cpu` + `exec`, with a 3-minute window) is tuned for interactive
sandboxes. A platform that wants AX-style suspend *during* model calls needs
the workload's cooperation, which is the `hint` signal (Phase 8): a local
socket where the harness says "parked until this call returns."

`suspend` is valid only on keyed stateful tasks in v1, and the error says
so:

```console
Error: task "api" sets `suspend` but is not a keyed stateful task
Hint:  suspend applies to keyed instances (stateful.mode = "keyed").
       for stateless services, scale with `legion job scale`
```

### 6.8 Primitive 7: content locality

This is the "snapshots are artifacts, and scheduling follows the bytes"
mechanism, built once for artifacts, images, and snapshots.

**Node content store.** One content-addressed store per worker at
`<state>/content/sha256/<digest>`, with the budget, LRU, TTL, and pinning
design of the artifact cache plan's Phase 1, unchanged. `ArtifactSink`
writes into it (the seam already exists). Snapshot parts are written with
an owner pin (the snapshot id) so LRU never evicts a checkpoint that a
suspended instance needs. Budgets are per class (`artifacts`, `snapshots`),
answering the old plan's open question 4: a 4 GiB memory file must not evict
every container layer on the node.

**Amendment 1 to the cache plan: a publish path.** Workers can upload. The
artifact fetchers stay read-only. A new `ContentPublisher` trait with an
`s3` (and `file`, for tests) implementation pushes a digest to the worker's
configured store:

```nickel
# worker config: each region's workers point at their own bucket,
# which is how region locality falls out without a manager-side map.
contentStore = {
    url = "s3://legion-content-usw2/",
    auth = { nodeIdentity = true },
},
```

A published snapshot's manifest records the store URL per part. Any worker
that can read that URL can restore it, and the restore fetch goes through the
ordinary `s3` fetcher, verified by digest. Cross-region replication stays
outside Legion, as in the old plan.

**Amendment 2: a location index instead of dynamic attributes.** The cache
plan proposed advertising cache contents as a `legion.artifact_cache`
dynamic attribute (`artifact-cache-and-prewarm-plan.md:310-345`). That puts
a node's content list into a replicated, attribute-matched column and
re-evaluates jobs on every change (`UpdateNodeDynamic` enqueues evals,
`src/manager/server.rs:6209-6217`). At snapshot scale (thousands of digests
per node, churning on every suspend) that is the wrong store. Instead:

- Workers send **deltas** (`content_added`, `content_removed`) in the
  heartbeat, with an epoch, and a full set on registration and on demand.
- Only entries of at least 1 MiB are advertised. Small blobs are not worth
  steering placement for.
- The manager holds `digest → {node: bytes}` **in memory, off raft**, like
  liveness. A new leader rebuilds it from full reports, and until it has a
  node's report that node simply scores 0 on locality. Losing the index costs
  placement quality for a few seconds, never correctness.

**The `locality` score term.** A placement's *content set* is the digests it
will need: pinned artifact digests, image layers, and the restore manifest's
parts. For a candidate node:

```
locality(node) = bytes of the content set already on node / total bytes
```

It joins the weighted sum as `schedulerSettings.weights.locality` (default
1.0 once Phase 7 ships, same scale as `affinity`). Placements with an empty
content set skip the term, so ordinary jobs pay nothing. The lookup is
`O(|content set|)` hash probes per candidate, bounded by the existing
bounded-candidate scoring, and lands with `scheduler_bench` before/after
numbers.

**Hard versus soft.** Locality is a *preference* everywhere except one case:
a checkpoint with `durability = "local"` exists only on its node, so resume
is a hard pin (§6.6). Published checkpoints are soft. A cold node can restore
them, it just takes longer.

**Durability `store`.** With `durability = "store"`, suspend is not complete
until the parts are published. The instance enters `suspended` when the
local write finishes (so capacity frees immediately) and the snapshot moves
`local → published` when the upload finishes. While `local`, the hard pin
applies. Once `published`, the instance can resume on any compatible node,
and node loss no longer loses the checkpoint. The node-local *volume* is a
separate question: a resumable-anywhere sandbox needs its volume to be
movable too, which means a snapshot-capable driver or a network volume. The
plan says this in the public doc rather than pretending otherwise.

**Pre-warm.** `legion snapshot warm <name> --constraint region=us-west` is
the cache plan's warm RPC applied to a snapshot's parts. It is best effort
and never blocks placement.

### 6.9 Composition: fork

```console
$ legion instance fork run-17 --count 3 -j --idempotency-key fork-7f2
{"ok":true,"data":{"origin":"run-17","snapshot":"snap-c0de",
 "pause_ms":610,"instances":["run-17-f1","run-17-f2","run-17-f3"]}}

$ legion instance create agents --from-snapshot py-ml-base --count 50
```

**Fork is three existing pieces and one new one.**

1. **Checkpoint the origin live.** `runtime.checkpoint(..,
   AfterCheckpoint::Resume)`: pause, capture, resume. The origin keeps its
   allocation and keeps running. Its pause is the time to write memory
   (about 0.5–1s per GiB on local NVMe for a full snapshot), reported as
   `pause_ms`. Disk is cloned with the volume `clone` op (reflink on
   `local-dir` when the filesystem supports `FICLONE`, otherwise a sparse
   copy), so the origin's volume is untouched.
2. **Create N keyed instances with `restore_from`.** Each gets a new id
   (never recycled), its own volume from `clone`, and its own workload
   identity. The at-most-one-live-allocation invariant is untouched, because
   a fork is a new instance, not a second allocation of the origin (old plan,
   Part III).
3. **Place near the bytes.** Before Phase 7 the snapshot is local, so forks
   are pinned to the origin's node. After Phase 7, `locality` prefers nodes
   holding the snapshot, and `--spread` adds a `distinct` node constraint
   for forks that should not share a failure domain.
4. **New: identity rebind on resume.** The guest agent grows a resume
   handler, specified in the old plan's Part III, run on every restore (not
   only forks): re-read identity from the identity disk, reseed the RNG
   (VMGenID where the VMM supports it, plus an explicit reseed from the
   identity disk), resync the clock, reconfigure the NIC for its new MAC/IP,
   and run the task's optional `onResume` command. The worker writes a fresh
   identity disk per restore with the new allocation id, instance id,
   hostname, and a newly issued workload identity token.

**Memory sharing and accounting.** Forks on one node restore from the same
memory file with `MAP_PRIVATE`, so unmodified pages are shared through the
page cache. Each fork is still **charged its full reservation**. Sharing is
real but unpredictable (it decays as forks diverge), and charging less would
make placement depend on a number that changes under it. The sharing shows
up as headroom that the ratio and the ladder can use.

`legion instance fork` is mutating, idempotent, and needs `snapshot:write`
on the origin's job as well as `job:write`, because a fork reads the
origin's memory.

### 6.10 Composition: wake on connect (sketch, Phase 8)

A suspended instance with a local checkpoint keeps its ingress port
reservation on its node. The worker holds a **parked listener** on that port.
On the first connection it holds the socket, asks the manager to resume the
instance with a node hint of itself, and splices the connection to the task
when it is ready, or closes it after `wakeOnConnect.timeout`. Service
discovery keeps the endpoint registered as `parked` rather than removing it.

This is deliberately a sketch. It touches the ingress datapath, discovery
semantics, and port reservation for an instance with no allocation. It gets
its own design review before Phase 8 executes. Platforms that front
sandboxes with their own router can call `StartInstance` today, which is
also how Fly and E2B are driven.

### 6.11 The agent use case, assembled

```nickel
# configs/jobs/agent-sandboxes.ncl
{
    name = "agents",
    priority = "normal",
    tasks = [{
        name = "workspace",
        runtime = "sandbox",
        group = "workspaces",
        stateful = {
            mode = "keyed",
            idSource = "caller",
            storage = { path = "/workspace", mode = "block", params = { size = "10G" } },
        },
        config = {
            image = "ghcr.io/acme/agent-base:2026-10",
            command = ["/usr/local/bin/harness"],
        },
        resources = { cpu = 1000, memory = 512, memoryMax = 4096 },
        suspend = {
            checkpoint = "memory",
            idle = { after = "3m" },
            onPressure = true,
            durability = "local",
        },
        identity = { audience = ["acme-api"] },
    }],
}
```

The platform's life, end to end:

| Platform step | Legion primitive |
| --- | --- |
| New user: `instance create agents --id u-42 --from-snapshot agent-golden` | 5 + 7: restore from a warmed snapshot, placed where it's cached |
| User works; guest bursts to 3 GiB during a build | 2: reservation 512, ceiling 4096 |
| Node gets tight during a peak | 3: reclaim over-reservation pages, then suspend idle sandboxes |
| User leaves for lunch | 6: idle for 3m → suspended, memory to local store |
| User returns: `instance start u-42` | 5: restore on the same node, identity refreshed |
| Agent tries three approaches: `instance fork u-42 --count 3` | 4 + 5 + 7: live checkpoint, three new instances |
| Weekly rebuild: snapshot a prepared instance, repoint alias `agent-golden` | 4 + 7: named snapshot, `snapshot warm` |

None of these rows needs an agent-specific concept. Each primitive is also
used by a non-agent workload somewhere in §3.

### 6.12 Security

- **New ACL resource `snapshot`** (`read`, `write`), scoped by job name and
  namespace like `job`. `snapshot:write` covers create, fork, suspend with a
  memory checkpoint, and delete. `snapshot:read` covers list, get, and
  restore-into-new-instance. `docs/auth-and-acls.md` states that
  `snapshot:write` on a job is equivalent to reading every secret that job
  has held in memory. Suspend with `checkpoint = "disk"` needs only
  `job:write`, like stop.
- **Identity refresh on every restore**, answering the old plan's open
  question 5. A restored guest's in-memory token carries the old allocation
  id and expires within its TTL (default 15m). The resume hook replaces it
  before `onResume` runs, and the `legion.io/alloc` claim on the new token
  names the new allocation. Tokens are not revocable today, so a snapshot
  captured less than one TTL ago contains a token that is still valid. The
  public doc says so, and short TTLs are the mitigation.
- **Memory at rest.** Checkpoint files are `0600`, owned by the worker, in
  the content store. Published parts rely on the bucket's server-side
  encryption in v1. Envelope-encrypting snapshot parts with the KV keyring
  is a follow-up (open question 4).
- **Restore never widens privileges.** A restore always uses the
  *destination* allocation's config (network policy, mounts, identity),
  never values from the manifest. This is the bug class that hit
  Kubernetes' implicit restore (§2.4), and the manifest's `compat` block is
  the only manifest data placement consumes.

### 6.13 Events, metrics, diagnostics

- Events: `instance.suspended`, `instance.suspend_failed`,
  `instance.resumed`, `instance.restore_failed`, `instance.forked`,
  `snapshot.created`, `snapshot.published`, `snapshot.deleted`,
  `node.pressure` (step, freed bytes). There are no `instance.*` events
  today, so this plan adds the topic.
- `allocation.removed` gains `cause = node_pressure` and
  `cause = suspended`.
- Metrics: per-node reserved/used/capacity memory, pressure actions by step,
  suspend/resume latency histograms, checkpoint bytes, content store bytes
  by class, locality hit ratio at placement.
- `instance get` shows the last suspend and restore with durations, the
  checkpoint's location and durability, and any restore failure.

### 6.14 Performance

- **Scheduler hot path.** The ratio is a capacity multiplication at
  registration. Compat constraints are ordinary constraints. The only new
  per-candidate cost is `locality`, skipped for empty content sets.
  Phases 2 and 7 ship with `scheduler_speed` and `candidate_construction`
  before/after numbers.
- **Heartbeat.** Usage is about 60 bytes per allocation every 10s, content
  deltas are typically empty, and neither touches raft.
- **Suspend and resume.** Targets on local NVMe, to be measured in Phase 4
  and published in `docs/testing-and-benchmarks.md`: suspend of a 1 GiB
  touched guest under 1.5s, resume under 300 ms (eager), resume from a
  published snapshot on a cold node under 3s for 1 GiB. Lazy memory
  (Phase 8) is what makes cold-node resume fast.
- **Idle sweep.** One manager pass every 15s over in-memory `idle_since`
  values, and suspends are enqueued like stops. No store reads.

---

## 7. Proto, struct, config, and persistence changes

**Proto (`proto/legion.proto`), all with public-facing comments:**

- `InstanceState` gains `SUSPENDED`. `InstanceInfo` gains `checkpoint_id`,
  `checkpoint_kind`, `checkpoint_durability`, `checkpoint_node_id`,
  `suspended_at`, `suspend_reason` (`manual | idle | pressure`),
  `restored_from_snapshot_id`, `last_restore_ms`, `usage`
  (`AllocationUsage`), `idle_since`.
- New RPCs: `SuspendInstance`, `ForkInstance`, `GetNodeUsage`, plus the old
  plan's `CreateSnapshot`, `ListSnapshots`, `GetSnapshot`, `DeleteSnapshot`,
  `RestoreInstance` (roll an instance back to a snapshot in place), and
  `WarmSnapshot`. `StartInstance` gains `cold: bool`. `CreateInstance(s)`
  gains `from_snapshot`. Every mutating one takes `idempotency_key` and
  reports `deduplicated`.
- `HeartbeatRequest` gains `allocation_usage`, `content_delta`,
  `suspend_results`, `local_suspensions`, `pressure_actions`.
  `HeartbeatReply` gains `suspend_allocations` and `checkpoint_requests`
  (live checkpoints for fork and manual snapshots).
- `TaskAllocation` gains `restore` (manifest + identity payload).
- Every new RPC goes into `tests/acl_endpoints.rs`, `catalog()` in
  `src/cmd/capabilities.rs`, and MCP tools in `src/cmd/mcp.rs`
  (`legion_instance_suspend`, `legion_instance_fork`,
  `legion_snapshot_*`, `legion_node_usage`).

**Jobspec (`configs/jobs/maximal.ncl` gets all of these):**

- `resources.memoryMax` (MiB, ≥ `memory`).
- Task `suspend` block: `checkpoint`, `idle { after, signals, cpuBelow }`,
  `onPressure`, `reserve`, `onRestoreFailure`, `durability`,
  `resumeTimeout`, `onResume` (argv run by the guest agent after restore),
  `quiesce` (old plan §4, for disk checkpoints on runtimes with actions).
- Task `snapshots { ttl, keep }` (old plan §2).
- Sandbox `config.memory_mib` becomes derived. An explicit value that
  disagrees with `memoryMax`/`memory` is a submit error.

**Worker config:** `resourceEnforcement.oversubscribe { memory, cpu }`,
`resourceEnforcement.memoryPressure { enabled, freeBelow, stallAbove, settle, steps }`,
`contentStore { url, auth }`, `content { budgets { artifacts, snapshots } }`,
`sandbox.vmmOverheadMiB`.

**Manager config:** `schedulerSettings.weights.locality`,
`suspend.idleSweep` (default 15s).

**Persistence (migrations):**

```sql
-- instance: the new state is a value of the existing `state` column
ALTER TABLE instance ADD COLUMN checkpoint_id TEXT;
ALTER TABLE instance ADD COLUMN suspend_reason TEXT;
ALTER TABLE instance ADD COLUMN suspended_at INTEGER;
ALTER TABLE instance ADD COLUMN pending_op TEXT;
ALTER TABLE instance ADD COLUMN restored_from_snapshot_id TEXT;

CREATE TABLE snapshot(
  snapshot_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES job ON DELETE CASCADE,
  task_name TEXT NOT NULL, origin_instance_id TEXT, name TEXT,
  kind TEXT NOT NULL,            -- disk | memory
  purpose TEXT NOT NULL,         -- suspend | manual | fork
  state TEXT NOT NULL,           -- capturing | local | published | lost | deleting
  node_id TEXT,                  -- where the local copy lives
  manifest_json TEXT NOT NULL, total_bytes INTEGER NOT NULL,
  created_at INTEGER NOT NULL, expires_at INTEGER);
CREATE TABLE snapshot_alias(job_id TEXT, name TEXT, snapshot_id TEXT,
  updated_at INTEGER, PRIMARY KEY(job_id, name));
```

A `suspend`-purpose snapshot is owned by its instance: it is deleted when
the instance resumes successfully or is destroyed, and is never subject to
`snapshots.ttl`. Node loss marks `local` snapshots on that node `lost`.

Usage, idle state, and the location index are **not persisted** (§6.2,
§6.8).

---

## 8. Implementation phases

Each phase ships alone, has value alone, and leaves the cluster in a
documented state.

**Phase 1: measure and govern.** Sandbox VMMs in the worker cgroup
(`ResourceEnforcement::WorkerCgroup`). Per-allocation usage in the heartbeat,
in-memory on the manager, `GetNodeUsage`, `legion node usage`, usage columns
in `instance list`, metrics. *Value:* every operator sees real usage, and
sandboxes stop being ungoverned.

**Phase 2: burst and oversubscribe.** `resources.memoryMax` with
`memory.low`/`memory.max` enforcement, sandbox guest size derived from
`resources`, worker `oversubscribe` ratios, the pressure steps `reclaim`,
`evict-bursting`, `evict-reserved` (no `suspend` yet), the pressure-order
preview in `node usage`, `cause = node_pressure`. Scheduler benchmarks.
*Value:* Nomad-parity overcommit with a better failure mode, for every
workload.

**Phase 3: checkpoints, local.** `TaskPlugin::checkpoint/restore`,
Firecracker `--api-sock`, cloud-hypervisor API client, identity disk slot,
the node content store (cache plan Phase 1, so artifacts get cached in the
same change), `snapshot` table, `legion snapshot create|list|get|delete|restore`
(restore in place for the same instance), compat fingerprints and
constraints, the `snapshot` ACL resource. The `local-dir` `snapshot` op
(reflink or copy) for disk snapshots. *Value:* checkpoint and roll back a
workspace (S5 in the old plan), plus a real artifact cache.

**Phase 4: suspend and resume.** `suspended` state, `SuspendInstance`,
resume through `StartInstance`, `--cold`, `onRestoreFailure`, `reserve`,
local-durability pinning, the guest agent resume hook with identity refresh,
disk-mode suspend for `exec` and `container`, `instance.*` events.
*Value:* S1 driven by a platform or an agent through the API. This is the
density win.

**Phase 5: automatic suspend.** The `idle` policy (worker detection, manager
sweep), the `suspend` pressure step (`onPressure`), balloon or free page
reporting on sandboxes for `reclaim`. *Value:* density without the platform writing its own
idle timer.

**Phase 6: fork.** `ForkInstance`, `CreateInstance(from_snapshot, count)`,
live checkpoint (`AfterCheckpoint::Resume`), the `local-dir` `clone` op,
named snapshots and aliases. Same-node forks only until Phase 7.
*Value:* S3.

**Phase 7: content locality.** Content publishing, `contentStore`, the
location index, the `locality` score term, `durability = "store"`,
`snapshot warm`, cross-node resume and fork. Executes the artifact cache
plan's Phase 3 with the §6.8 amendments. Scheduler benchmarks. *Value:* S4,
suspended instances that survive node loss, and locality for every artifact
and image.

Phase 7 splits in two. **7a** is everything that needs no snapshots, and
shipped: `ContentPublisher` (`s3`, `file`) and the worker `contentStore`
block, the location index with epoch deltas on the heartbeat, and the
`locality` score term at weight 1.0 in both scoring paths, with a
`Scheduler::restore_content` hook for manifest parts. **7b** is
`durability = "store"`, `snapshot warm`, and cross-node resume and fork,
which need Phases 3c and 4, and shipped too. Fork's cross-node placement is
the generic rule for any instance with `restore_from`, so it applies once
Phase 6 lands. A published checkpoint needs instance data that can move:
node-local volumes are refused, not migrated (calls entry 8). Image layers join the content set once the
content store holds OCI blobs. Calls 32, 78, 80, 89, 90, 91, 105, 106 record the 7a decisions.

Phases 6 and 7 depend only on Phase 3 and 4, not on each other, and can run
in parallel.

**Phase 8: fast paths, each its own review.** Wake on connect (§6.10), the
`hint` idle signal, lazy memory (old plan Part II.2, UFFD), diff snapshots.

Phases 1–2 are the general-purpose product, 3–5 are the agent core, and 6–8
are reach.

---

## 9. Testing plan

Everything that needs real Linux (KVM, cgroup v2, `/proc`) and could not run
on the macOS dev machine is tracked, with commands and pass criteria, in
[`../suspend-resume-linux-verification.md`](../suspend-resume-linux-verification.md).

**Phase 1**
- Unit: cgroup readers against fixture `memory.stat`/`cpu.stat`/`memory.pressure`.
- Integration (Linux, cgroup v2): a sandbox's VMM appears in its allocation
  cgroup and `memory.max` is guest + overhead. An `exec` task allocating
  300 MiB shows ~300 MiB in `GetNodeUsage` within two report intervals.
- Regression: heartbeats without usage (old workers) still parse, and
  manager restart drops usage without errors.

**Phase 2**
- Unit: `ResourcePool::from_node` with ratios, the cached `NodeUsage`
  fingerprint invalidates on ratio change, `memoryMax < memory` rejected,
  sandbox `memory_mib` disagreement rejected.
- Ladder unit tests with a fake pressure source: candidate order by band
  then bytes, node-pinned storage never evicted, `evict-reserved` never
  runs at ratio 1.0, a step removed from `steps` never runs.
- Integration: two `exec` tasks bursting past reservation on a node with a
  small watermark. The lower band is evicted with `cause = node_pressure`,
  the higher survives.
- Benchmarks: `scheduler_speed` and `candidate_construction` before/after.

*Phase 2 results (2026-10-10, base 1a65654 vs. the Phase 2 commit, Apple
Silicon dev machine).* Criterion medians in ms, `--sample-size 10`, the
`allocs_100000` rows skipped (one sample took over a minute). **The machine
was shared with other agents building and benchmarking (load average 4 to
60), so these are noise-dominated and not PR-grade.** The two runs moved in
opposite directions on the same code path (bin_pack geomean 0.63x, spread
geomean 1.32x), which is load drift, not the change. The change adds one map
lookup per `ResourcePool::from_node` and one string compare per resource key
in `ResourceRequest::from_value`, and nothing per candidate. Rerun on a quiet
host before the PR.

| Benchmark | Before | After |
| --- | --- | --- |
| `candidate_construction/indexes_off/nodes_1000` | 2.183 | 1.570 |
| `candidate_construction/indexes_on/nodes_1000` | 3.455 | 1.401 |
| `candidate_construction/indexes_off/nodes_5000` | 9.571 | 5.718 |
| `candidate_construction/indexes_on/nodes_5000` | 6.373 | 3.679 |
| `candidate_construction/indexes_off/nodes_10000` | 15.260 | 13.306 |
| `candidate_construction/indexes_on/nodes_10000` | 19.947 | 14.459 |
| `scheduler_speed/bin_pack/nodes_1000_allocs_1000` | 162.850 | 84.335 |
| `scheduler_speed/bin_pack/nodes_10000_allocs_1000` | 134.290 | 105.290 |
| `scheduler_speed/bin_pack/nodes_10000_allocs_10000` | 7808.6 | 16498.0 |
| `scheduler_speed/spread/nodes_1000_allocs_1000` | 98.935 | 100.910 |
| `scheduler_speed/spread/nodes_10000_allocs_1000` | 285.090 | 317.470 |
| `scheduler_speed/spread/nodes_10000_allocs_10000` | 14050.0 | 11186.0 |

**Phase 3** (Linux steps: [Linux verification plan,
Phase 3a](../suspend-resume-linux-verification.md#phase-3a) for the content
store, [Phase 3b](../suspend-resume-linux-verification.md#phase-3b) for the
runtime slice, [Phase 3c](../suspend-resume-linux-verification.md#phase-3c)
for the snapshot API and restore)
- Volume harness extends with `snapshot` and double-call idempotency for
  `local-dir` in both modes (no KVM).
- Round trip (exec, no KVM): write marker → disk snapshot → mutate →
  restore → marker is back.
- Ignored KVM tests (like `sandbox_microvm_boots`): memory checkpoint and
  restore of a guest holding a counter in RAM, on both VMMs.
- Compat: a manifest with a mismatched `cpu.model` fails placement with a
  diagnostic, never a corrupt guest.
- Content store: LRU never evicts a pinned snapshot, and per-class budgets
  are honored.

**Phase 4** (KVM steps: [Linux verification plan,
Phase 4](../suspend-resume-linux-verification.md#phase-4))
- State machine: suspend/start/stop/destroy interleavings, double suspend
  with one idempotency key, suspend of a `stopped` instance rejected.
- Failure: checkpoint failure leaves the allocation running, restore failure
  with `cold` boots on the volume, with `fail` stays suspended.
- Node loss with a local checkpoint marks the snapshot `lost`, and `start`
  follows `onRestoreFailure`.
- Identity: a restored guest's token names the new allocation id.
- `acl_endpoints` covers every new RPC.

Phase 4 shipped with these as `tests/suspend_integration.rs` (the manager
side, against a simulated worker), `an_exec_disk_suspend_cold_boots_on_its_retained_volume`
and `a_suspend_that_cannot_capture_leaves_the_task_running` in the worker (a
real `exec` task), `a_restored_guest_gets_a_token_naming_the_new_allocation`
(the token chain), and `guest_resume` unit tests against a fake guest. The
KVM latency target and the in-guest refresh are in the verification plan.
Two deviations: allocation ids became monotonic (calls entry 26), and the
memory suspend keeps the guest paused through the content-store ingest
(calls entry 29).

*Phase 4 scheduler results (2026-10-10, base f3ea60c, the 3c and 7a merge,
vs. the Phase 4 commit, Apple Silicon dev machine, `--sample-size 10`,
medians in ms).* The scheduler changes are the restore lookup moving ahead of
the locality term (so `restore_content` can read it) and the keyed desired
count counting held reservations. The bench jobs are stateless, so neither
path runs here beyond one empty slice per task. **The host was loaded (load
average 16 to 28) and the control rows moved as much as anything:
`candidate_construction` never reaches either change and slowed 7 to 75%.
Treat these as a smoke test and rerun on a quiet host before the PR.**

| Benchmark | Before | After |
| --- | --- | --- |
| `scheduler_speed/bin_pack/nodes_1000_allocs_1000` | 68.05 | 83.92 |
| `scheduler_speed/spread/nodes_1000_allocs_1000` | 74.03 | 94.55 |
| `candidate_construction/indexes_off/nodes_1000` (control) | 1.005 | 1.637 |
| `candidate_construction/indexes_on/nodes_1000` (control) | 1.048 | 1.789 |
| `candidate_construction/indexes_off/nodes_5000` (control) | 4.085 | 7.177 |
| `candidate_construction/indexes_on/nodes_5000` (control) | 2.727 | 2.906 |
| `locality/content_absent/nodes_1000` | 1.065 | 1.107 |
| `locality/content_present/nodes_1000` | 1.181 | 1.254 |
| `locality/content_absent/nodes_5000` | 2.289 | 2.472 |
| `locality/content_present/nodes_5000` | 2.966 | 3.083 |

**Phase 5** (Linux steps: [Linux verification plan,
Phase 5](../suspend-resume-linux-verification.md#phase-5))
- Idle detector unit tests per signal, including "quiet open connection"
  with and without `connections`.
- Integration: an idle sandbox suspends after `after`, a busy one never
  does, and a pressured node suspends opted-in idle instances before
  evicting anything.

Phase 5 shipped with these as `src/worker/idle.rs` (one test per signal, the
quiet-connection pair, the `cpuBelow` scaling, the idle-since tracker),
`idle_since_follows_the_policys_signals` in the usage sampler,
`an_idle_instance_suspends_after_its_window_and_a_busy_one_never_does` and
the local-suspension tests in `tests/suspend_integration.rs` (manager side,
fake usage and simulated reports),
`opted_in_instances_are_suspended_most_idle_first_before_evict_bursting` in
`src/worker/pressure.rs` (a fake node), a pressure suspend of a real `exec`
task in the worker, and the balloon against fake Firecracker and
cloud-hypervisor API servers. The idle "sandbox" is an `exec` task with fake
usage: a real idle suspend needs cgroups, so it is in the verification plan.
Phase 5 made no scheduler change, so it has no benchmark numbers. Open
question 3 is answered by calls entry 28, and §7's
`local_suspensions` / `suspend_allocations` heartbeat fields were not needed
(calls entry 17).

**Phase 6**
- Fork divergence: three forks have distinct instance ids, hostnames, MACs,
  `/dev/urandom` reads, UUIDs, and volumes, and the origin keeps running.
- Idempotent fork: the same key twice yields one set of forks.

Phase 6 shipped with the manager half end to end against a simulated worker
(`tests/fork_integration.rs`: three forks wait for the checkpoint and run as
distinct instances with their own allocations and volumes, idempotent retry,
never-recycled ids, a failed capture, the refusals, named snapshots, and
`onRestoreFailure`), a real `exec` disk fork with three `local-dir` clones
(`an_exec_disk_fork_gives_every_fork_its_own_copy_of_the_volume`), and the
divergent identities, MACs, and TAPs against fakes. The `/dev/urandom` check
is `sandbox_fork_divergence_*`, ignored without KVM, and listed with the
rest in the [Linux verification plan](../suspend-resume-linux-verification.md#phase-6).
The plan's UUID check folds into the RNG one: a guest's UUIDs come from its
RNG.

**Phase 7**
- Location index: deltas, epochs, full resync after manager failover, and
  scoring at 0 before a node reports.
- Locality: two nodes, one holding the content, the placement lands on the
  warm node with weights equal otherwise.
- Published resume: suspend on node A with `durability = "store"`, kill A,
  resume on B.
- Benchmarks before/after with content sets present and absent.

Phase 7a shipped with the location index, publisher, and score tests above
(`src/storage/content_index.rs`, `src/worker/content_report.rs`,
`src/content_publish.rs`, `tests/content_locality.rs`, and
`placement_prefers_the_node_holding_the_content` in the scheduler). The
published-resume test is 7b. The `scheduler_bench` `locality` group measures
content present and absent. One `plan_only` of 100 placements (4 tasks x 25), bin_pack, half the fleet
holding one digest and a quarter the other, medians:

| Cell | 1,000 nodes | 5,000 nodes |
|---|---|---|
| `content_absent` (run 1 / run 2) | 1.03 / 1.18 ms | 2.28 / 3.05 ms |
| `content_present` (run 1 / run 2) | 1.17 / 2.04 ms | 3.30 / 2.93 ms |

Run 1 copied the score map into the scoring cache per task. Run 2 shares
it through an `Arc`. The run-to-run spread is larger than the difference
between cells, so the only defensible reading is that the term costs at
most a few hundred microseconds per 100-placement plan at 5,000 nodes and
nothing when the content set is empty (the `content_absent` cells skip the
lookup entirely).
**A quiet-host before/after run is still pending**: these numbers came from
a shared machine under load from another session, so treat them as a smoke
test, not the PR's evidence.

Phase 7b shipped the published-resume test as
`tests/published_resume_integration.rs` (suspend on A with `durability =
"store"`, publish, remove A or mark it lost, resume on B; locality prefers
the node holding the parts; `compat` filters every candidate; warm), the
worker transport for real with the `file` publisher
(`a_published_snapshot_restores_on_a_node_that_never_held_it`,
`warm_fetches_published_parts_into_the_store_once` in
`src/worker/task_manager.rs`), and `tests/restore_placement_indexed.rs` for
the truncation fix (calls entry 80). It added a `restore` group to
`scheduler_bench`: 25 keyed instances restoring from their own `local`
snapshots (`local_pinned`) or `published` ones (`published_floating`, compat
on every candidate, parts seeded on a quarter of the fleet). One run
(2026-10-10, Apple Silicon, load average 10 to 22 from other sessions,
criterion medians):

| Cell | 1,000 nodes | 5,000 nodes |
|---|---|---|
| `locality/content_absent` | 1.24 ms | 2.34 ms |
| `locality/content_present` | 1.25 ms | 3.75 ms |
| `restore/local_pinned` | 1.08 ms | 7.07 ms |
| `restore/published_floating` | 4.58 ms | 26.9 ms |

There is no "before" for `restore`: on the base commit the `local_pinned`
cell fails at 1,000 nodes, because the armed index truncated the pinned node
out of the candidate list (the bug calls entry 80 fixes). A floating restore
scores and compat-checks the whole fleet for each placement, about 1 ms per
placement at 5,000 nodes. Restores are rare, so this is acceptable, but the
coordinator's clean pass should rerun both groups.

---

## 10. Rollout and backward compatibility

- **Everything is opt-in.** No `memoryMax`, no ratio, no `suspend` block,
  and no new config means identical behavior. The `locality` weight
  defaults to 0 until Phase 7 ships, then 1.0, and it only affects
  placements with a content set.
- **Sandbox enforcement (Phase 1) is gated by the existing worker
  `resourceEnforcement.enabled`.** Clusters that left it off see no change.
  Clusters that turned it on will start governing sandboxes. That is the
  one behavior change for existing users, and the release notes call it out
  with the overhead knob.
- **Sandbox `memory_mib` derivation (Phase 2).** Existing specs that set
  `memory_mib` without `memoryMax` keep working if `memory_mib == memory`.
  If not, submit fails with the fix. This can break a resubmit of an
  existing job, so Phase 2 ships one release where the mismatch is a
  warning, then an error.
- **Mixed versions.** A manager will not send `suspend_allocations` or a
  `restore` payload to a worker that does not advertise the
  `checkpoint` capability at registration. Such an instance fails suspend
  with `FailedPrecondition` naming the worker version. Old workers ignore
  unknown heartbeat fields.
- **Upgrading a VMM** invalidates memory checkpoints whose `compat` no
  longer matches. With `onRestoreFailure = "cold"` this degrades to a cold
  boot, and `docs/upgrades.md` gets a "drain suspended instances before a
  VMM upgrade" note.
- **Migrations** are additive columns and new tables. `instance.state` gains
  a value, so a downgraded manager would see an unknown state. The upgrade
  doc says to resume or stop suspended instances before downgrading.

---

## 11. Alternatives considered

### A. Generic primitives in core (recommended)

The design above. It costs more surface area spread across more
subsystems, and the agent story only lands when several phases have
shipped. It wins because every primitive pays for itself with non-agent
users, and the scheduler stays the single source of placement truth.

### B. A Substrate-style actor runtime

Add a separate "actor" object multiplexed onto pre-started worker
allocations by a node-local runtime, with its own lifecycle and API. It
ships agent density faster and isolates the risk. It loses because it
duplicates instances, stateful storage, identity, and placement, and creates
a second scheduler that the first one cannot see. That is the exact
complexity Legion's pitch is against.

### C. Keep suspend outside Legion

The old plan's stance: Legion provides fast `create --from-snapshot`, and
platforms run idle timers and pools. It is the smallest change. It loses
because the scheduler then cannot release a suspended instance's capacity
while still knowing it exists, prefer its node on resume, or use suspend as
a pressure valve. Those are the three things that make density safe (E2B on
Nomad is the cautionary example).

### D. CRIU for `exec` and `container`

It would give every runtime memory suspend. It loses for now on reliability
(TCP state, GPUs, namespaces, kernel version coupling) and on security
(Kubernetes is disabling implicit restore after CVEs, §2.4). The
`CheckpointSupport` seam leaves room for a CRIU-backed runtime plugin later.

### E. Usage-based scheduling instead of ratios

Schedule on measured usage plus headroom (Borg-style reclamation). It's
denser and adapts automatically. It loses for v1 because it makes placement
depend on a noisy, unreplicated signal, and Legion has no usage history yet
to tune it on. Phase 1 starts collecting that history.

---

## 12. Open questions

1. **Suspend for slot instances.** A suspended slot leaves the pool short,
   and rollouts would need to skip or resume it. *Lean:* keep keyed-only
   until a concrete slot use case appears.
2. **`hint` signal transport.** Inside microVMs it rides the guest agent's
   vsock. For `exec` and `container`, a unix socket in the alloc dir.
   *Lean:* one tiny line protocol (`busy`, `idle`, `idle-until <deadline>`)
   for both, specified with Phase 8.
3. **Balloon versus free page reporting** for the `reclaim` step on sandboxes. Free page
   reporting is automatic but needs guest kernel support. Balloon inflation
   is explicit but needs a target. *Lean:* free page reporting by default
   where the guest kernel has it, balloon as the explicit pressure lever.
   Verify against the VMM versions we pin before Phase 5. *Answered in
   Phase 5:* both, with free page reporting on cloud-hypervisor only until
   the pinned Firecracker is confirmed to accept it (calls entry 28).
4. **Encrypting published snapshot parts** with the KV keyring's data keys.
   *Lean:* yes, as a follow-up plan, because the keyring rotation and
   rewrap story would need to cover multi-GiB blobs.
5. **Default `idle.after`.** Three minutes fits interactive sandboxes but
   may be wrong for agent harnesses. *Lean:* keep 3m and revisit with data
   from Phase 5.
6. **Should `--cold` keep the checkpoint** for later inspection?
   *Lean:* no, delete it. A responder who wants it can `snapshot create`
   from the restored instance.

---

## 13. Acceptance criteria

- **Phase 1:** `legion node usage <node> -j` shows per-allocation memory
  within 10% of `memory.current` for exec, container, and sandbox tasks. A
  sandbox's VMM is in its allocation cgroup. Usage never appears in the
  state store.
- **Phase 2:** a node with `oversubscribe.memory = 1.5` admits 1.5× its
  memory in reservations. A burst past the watermark evicts the lower-band,
  over-reservation allocation with `cause = node_pressure` and a hint, and
  never a within-reservation allocation at ratio 1.0. Benchmarks are in the
  PR.
- **Phase 3:** `legion snapshot create user-42 --memory` then
  `legion snapshot restore` brings back a RAM-only counter on both VMMs. A
  compat mismatch fails placement with a named reason. Artifacts are served
  from the node store on the second allocation.
- **Phase 4:** `legion instance suspend` frees the reservation (visible in
  `node usage`), `instance start` restores on the same node in under 300 ms
  for a 1 GiB guest on the reference hardware, and the restored guest's
  token names the new allocation. An `exec` task suspends and cold-boots on
  its retained volume.
- **Phase 5:** an idle sandbox suspends within `after` + 30s, a busy one
  never does, and a pressured node suspends opted-in idle instances before
  `evict-bursting` stops anything.
- **Phase 6:** `instance fork --count 3` returns three ids in one envelope,
  the origin keeps running, and the forks differ in id, MAC, hostname, and
  RNG output. Retrying with the same key creates nothing new.
- **Phase 7:** a `durability = "store"` instance resumes on another node
  after its original node is destroyed. A placement with a content set
  prefers the warm node. Benchmarks are in the PR. The artifact cache
  plan's Phase 3 is checked off in TODO.md.
- **Every phase:** public docs updated (`docs/suspend-and-resume.md` new,
  plus `resource-enforcement.md`, `preemption-and-eviction.md`,
  `task-runtime-sandbox.md`, `artifacts.md`, `stateful-workloads.md`,
  `auth-and-acls.md`, `agent-readiness.md`, `event-stream.md`,
  `metrics.md`), `maximal.ncl` updated, TODO.md updated, and judgement calls
  recorded.
