# Sandboxes at Scale

This page is for platforms that run many sandboxes: an agent workspace per
user, a microVM per untrusted job, a dev environment per branch. It covers
how Legion models them, what each step of a sandbox's life costs the
control plane, how fast work reaches a node, what happens when capacity runs
out, and how to measure all of it on your own hardware.

> Related: [Stateful workloads](stateful-workloads.md) (instances and the
> `keyed` mode this page builds on),
> [the sandbox runtime](task-runtime-sandbox.md) (the microVM itself),
> [Suspend and resume](suspend-and-resume.md),
> [Edge boot](edge-boot.md) (starting before the placement commits),
> [Scheduler and eval loop](scheduler-and-eval-loop.md#blocked-evals).

## The model

A sandbox is a **keyed instance** of a stateful task, usually on the
[`sandbox` runtime](task-runtime-sandbox.md). The job defines what a sandbox
is (image, size, storage, suspend policy); your platform creates and destroys
sandboxes through the management API:

```console
$ legion instance create agents --id user-42 -j        # CreateInstance
$ legion instance suspend user-42                       # SuspendInstance
$ legion instance start user-42                         # resume: StartInstance
$ legion instance destroy user-42                       # DestroyInstance
```

Each sandbox is a row in the replicated state store, with these guarantees
([Stateful workloads](stateful-workloads.md#keyed-mode-the-management-api)):

- **An identity that survives failures.** The id is stable across node loss,
  drain, and reschedule, and it is never recycled while the job exists, so a
  new sandbox can never inherit a dead one's state.
- **At most one live copy.** Every placement binds inside the transaction
  that inserts it; a lost node's sandbox is replaced only once the
  `onLost` policy says the old one is gone.
- **Durable intent.** Once `CreateInstance` returns, the sandbox will be
  placed: through a manager failover, and through a full cluster (it waits
  [blocked](#when-capacity-runs-out) and places when room appears).
- **Suspend, resume, and fork,** from memory or disk checkpoints, local or
  published to a content store ([Suspend and resume](suspend-and-resume.md)).

Choose `keyed` over a plain `count` when sandboxes come and go on demand and
each one is a distinct thing someone can name and come back to; see
[Choosing a mode](stateful-workloads.md#choosing-a-mode).

> **Keyed or session?** Use `keyed` when each sandbox must survive its
> node dying (it is rescheduled with its storage), when sandboxes keep
> volumes or need suspend and resume, or when you need an exact, durable
> record of every sandbox: per-user workspaces people come back to. Use
> `session` when sandboxes are disposable, created by the thousands per
> second, and numerous enough that a row per sandbox would weigh on the
> control plane: per-task agent sandboxes, tool executions, CI. A session
> that turns out to matter can be promoted to a keyed instance without
> stopping it. The trade-offs are in [Session mode](session-mode.md).

## What each step costs

The control plane's cost is raft entries: each is a quorum round on one
lane, and the scheduler lane runs them one at a time. On an idle cluster, a
sandbox's life costs:

| Step | Raft entries | What they are |
| --- | --- | --- |
| Create | 1 | the instance row and its placement eval, in one write |
| Place | 1 | the plan: allocation, instance bind, and the eval's settle together |
| Running (first heartbeat) | 1 | the allocation's first health report |
| Running (steady heartbeats) | 0 | health is written only when it changes |
| Suspend | 2 | the suspend, then the checkpoint report that stops the allocation |
| Resume | 3 | the resume, its placement, and the restore report |
| Fork (two copies) | 4 | the fork, its checkpoint, and the two placements |
| Destroy | 1 | the allocation removal and the tombstone (or, for an id Legion minted, the row's deletion), in one write |
| **Create → place → suspend → resume → destroy** | **8** | the five lifecycle steps; the same with idempotency keys on every call |

Heartbeats and forks are listed beside the lifecycle, not in its total. A
task's first instance costs one more entry, once, when the task's status
flips to running. `tests/raft_lifecycle_ops.rs` asserts every row of this
table on both raft topologies, so a change that adds an entry fails the
build.

Under load, concurrent instance operations share writes: creates, stops,
starts, destroys, suspends, resumes, forks, and edge ratifications that
arrive together commit as one entry between them, each isolated in its own
savepoint so one failure rolls back alone
(`schedulerSettings.instance_group_commit`, on by default).

## How fast work reaches a node

A node holds one `StreamWork` stream to its manager, and every placement
reaches it **one network round trip after it commits**, whatever the
heartbeat interval ([Delivery](architecture-managers-workers.md#delivery)).
A placement does not wait for a push to land or for the next heartbeat.

Dense nodes stay cheap. A node's heartbeat carries its stream position and
a digest instead of its allocation list, and the reply carries no desired
set: a steady beat is a few dozen bytes each way whether the node runs ten
sandboxes or ten thousand, where the full set it replaced was 180 KB up and
4.4 MB down at 10,000 allocations
([Heartbeat cadence and scale](heartbeat-cadence-and-scale.md#what-a-beat-carries-and-what-it-no-longer-gates)).

For the lowest start latency, [edge boot](edge-boot.md) starts a sandbox on
a node before the placement commits, from a signed boot ticket, and
ratifies it in one write (a ticket that names a sandbox that does not exist
yet creates it in that same write).

## When capacity runs out

A create that finds no room does not fail. The sandbox stays `pending`, its
job's eval waits **blocked** under what it needs (the task's pool and pinned
attributes), and it places on its own the moment capacity that could host it
appears: a node joining that pool, a node uncordoned, or sandboxes leaving a
node (a destroy, a suspend). A node joining another pool wakes nothing.
`legion job info <job> -j` says what the job waits for:

```json
"blocked": {"key": "class:pool=agents", "task": "workspace",
            "shortfall": {"cpu": "500MHz", "memory": "3000MiB"}, ...}
```

The full model is in
[Blocked evals](scheduler-and-eval-loop.md#blocked-evals).

## How big the store gets

Every manager holds the whole replicated store, and a manager that joins, or
a follower that falls behind, receives all of it as a snapshot. So the
number that limits how many sandboxes a cluster can have is what each one
leaves in the store, and the table below is what Legion keeps for each
state. Sizes are from `tests/instance_scale_measure.rs`.

| Sandbox | What the replicated store keeps |
| --- | --- |
| Pending keyed instance | An instance row, about 216 bytes |
| Running keyed instance | The row plus its allocation, about 231 bytes more |
| Suspended keyed instance | The row plus its checkpoint record, about 1.45 KB more |
| Suspended, `suspend.archive = "slim"` | The instance row only, naming the archive manifest in the bucket |
| Suspended, `suspend.archive = "manifestOnly"`, id minted by Legion | Nothing: the [archive manifest](suspend-and-resume.md#archiving-idle-instances) in the bucket is its record |
| Destroyed, id you chose | A `terminated` tombstone, so the id is never reused |
| Destroyed, id minted by Legion | Nothing: the id is 130 random bits ([Id rules](stateful-workloads.md#id-rules)) |
| Running [session](session-mode.md) | Nothing: its node runs it and the managers index it in memory |

Two of those rows are what makes a store grow without bound: tombstones,
which accumulate with every sandbox ever destroyed, and checkpoint records,
which accumulate with every sandbox parked. A platform that lets Legion
mint ids and archives idle sandboxes with `manifestOnly` keeps a store the
size of its *running* set: 100,000 minted sandboxes created and destroyed
around 1,000 live ones leave exactly 1,000 rows.

The other limit is the start rate. Every keyed start is a write to the
replicated log, whose scheduler lane commits one entry at a time; that
bounds keyed sandboxes at about 50 full lifecycles a second on a 4-vCPU
reference host ([Limits](#limits-to-know-about)). A session start skips
the log, so session starts are bound by the managers' routing and the
nodes, not by the lane: one manager on the same host started about 970
sessions a second across 16 nodes, with no replicated write
([Session mode](session-mode.md)).

## Measured on AWS

How fast Legion boots [session](session-mode.md) sandboxes on real KVM,
measured with the rig in
[`benchmarks/aws/sandbox-boot/`](../benchmarks/aws/sandbox-boot/README.md),
whose [results file](../benchmarks/aws/sandbox-boot/RESULTS.md) has every
run. The sandbox is Alpine on cloud-hypervisor v51.2, 1 vCPU and 128 MiB,
with no NIC. Workers are c8i instances with nested virtualization unless
stated, one 2-vCPU manager routes, and "ready" means the guest's agent takes
exec: the moment a caller can run code in it.

| | Result |
| --- | --- |
| One boot, create to ready | 179 ms p50 with the image pinned by digest and worker state on XFS; 229 ms by tag; 212 ms on bare metal |
| Burst, four 4-vCPU workers | 160 sandboxes ready in 2.33 s (69 a second), p99 0.74 s |
| Sustained, four 4-vCPU workers | 40 boots a second with none refused (p99 0.45 s); about 56 a second at the ceiling |
| Sustained, one 16-vCPU worker | 40 a second with none refused; about 48 at the ceiling, with the CPU 92% busy |
| Host-process sessions through one 2-vCPU manager | about 1,300 starts a second on four workers, bound by the workers |
| Manager memory per running session | about 1.2 KB, so 1.2 GB per million |
| Memory per idle guest on the node | about 70 MiB of the 128 MiB it was given |

Against `origin/main` before this work, on the same four workers: one boot
went from 2.9 s to 0.23 s, the burst of 160 from 98 s to 2.3 s (1.6 to 69
guests a second), and a sustained 20 a second from 69% refused to none.

**What bounds a node.** On small and mid-size nodes the CPU does: about 3
boots a second per vCPU under nested virtualization, about half of it guest
boot and a quarter the host kernel handling VM exits, which bare metal
mostly removes. A large node is bound by the worker instead: a 96-vCPU bare
metal node booted about 50 a second while its CPUs sat about 90% idle,
because starts waited on the worker's own state lock and on process spawns.
That ceiling, about 50 boots a second per node whatever its size, is the
known limit to raise next ([Limits](#limits-to-know-about)).

**What bounds a cluster.** Starts at the edge (`Worker.StartSession`) reach a
node with no manager on the path, so a cluster's boot rate is the sum of its
nodes'. Starts routed by managers are also bound by the router: a pick costs
31 µs with `stateful.placement = "spread"` on 1,000 nodes and 330 µs on
10,000, or a fraction of a microsecond with `"fast"` at any size, and one
manager routed 36,000 starts a second in process (on a laptop, to fake
nodes).

### Projections

These multiply the per-node numbers above. They are estimates, not
measurements of clusters that large.

| Fleet | Sustained boots a second | Sandboxes running at once (128 MiB each) |
| --- | --- | --- |
| 100 nodes of 16 vCPU, 32 GiB | about 4,800 | about 19,000 by envelope, about 35,000 by measured memory |
| 1,000 nodes of 16 vCPU, 32 GiB | about 48,000 | about 190,000 to 350,000 |
| 1,000 bare-metal nodes of 96 vCPU, 192 GiB | about 50,000 today (the per-node ceiling); about 290,000 if the CPU becomes the limit | about 1.2 to 2.1 million |

At 48,000 boots a second, a million sandboxes start in about 21 seconds, and
the managers holding a million running sessions keep about 1.2 GB each in
their session index. A thousand nodes heartbeating every 2 seconds cost a
manager about 4 ms of CPU a second.

### Compared with other systems

Each figure is the system's own, under its own conditions (hardware, image,
kernel, and what "started" means differ), so read them as orders of
magnitude, not a benchmark.

| System | Start latency | Start rate |
| --- | --- | --- |
| Legion (this page) | 0.18 to 0.23 s from create to a microVM taking exec | about 48 a second per 16-vCPU node; a cluster's rate is the sum of its nodes |
| Firecracker, as quoted | about 125 ms from VMM launch to guest user space | up to 150 microVMs a second per host ([summary](https://northflank.com/blog/what-is-aws-firecracker)) |
| Modal sandboxes (v2, 2026) | under 0.5 s median, request to user code | 1 million in under a minute, on "many tens of thousands" of nodes ([article](https://youmind.com/landing/x-viral-articles/scaling-1-million-concurrent-sandboxes)) |
| E2B | about 80 ms to load a sandbox from a memory snapshot ([docs](https://e2b.dev/docs/template/how-it-works)) | |
| Kubernetes | upstream SLO: p99 under 5 s from pod creation to containers started, image pulls excluded ([EKS](https://docs.aws.amazon.com/eks/latest/best-practices/kubernetes_upstream_slos.html)) | |
| Nomad (C1M, 2016) | | 1 million containers scheduled in 267 s on 5,000 hosts, 99% running at 370 s ([HashiCorp](https://hashicorp.com/c1m)) |

Legion's starts go to a node with no write to the replicated store and no
scheduler on the path, the shape Modal's v2 describes, and they boot fresh:
the numbers above use no memory snapshot or warm pool, which is where E2B's
80 ms comes from.

## Measuring it on your hardware

`legion-loadgen --mode instances` drives sandbox lifecycles at an open-loop
rate against any cluster and prints p50/p99 latency per step
([Testing and benchmarks](testing-and-benchmarks.md#keyed-instance-lifecycles---mode-instances)).
It embeds simulated nodes with the checkpoint channel faked, so it needs
only a manager:

```bash
legion-loadgen --mode instances --manager http://<manager>:4747 \
    --simulate 50 --sessions 2000 --rate 100
```

On AWS, `benchmarks/aws/scripts/run-sandbox-campaign.sh` runs the same
against a 3-manager raft cluster on four on-demand boxes for about a dollar
an hour, climbs a ladder of rates to the ceiling, holds thousands of
sandboxes resident, and writes a report with the ceiling, latencies, raft
entries and manager CPU per lifecycle, manager memory per resident sandbox,
and labelled projections (lifecycles per day, control-plane cost per
million lifecycles). See `benchmarks/aws/README.md`, "Sandbox-scaling
campaign".

Both measure the control plane's share of a sandbox start. Add the
runtime's own boot or restore time for the end-to-end figure.

## Limits to know about

- **A node boots about 50 sandboxes a second at most, whatever its size.**
  Small and mid-size nodes reach their CPU first. On a 96-vCPU bare-metal
  node, starts wait on the worker's state lock and on process spawns while
  the CPUs idle, so add nodes rather than cores to raise a cluster's boot
  rate ([Measured on AWS](#measured-on-aws)).
- **Every keyed sandbox is a row.** A named one leaves a tombstone after
  destroy for the life of its job; a server-minted one leaves no row
  ([Id rules](stateful-workloads.md#id-rules)). A pending instance row is
  about 216 bytes (a suspended one about 1.7 KB with its checkpoint
  record); 10^6 instances is a ~217 MB store image, which every new manager
  and lagging follower receives. Archiving an idle instance shrinks it to a
  slim row, or to none for a server-minted id
  ([Archiving idle instances](suspend-and-resume.md#archiving-idle-instances)),
  and a [session](session-mode.md) has no row while it runs.
- **Suspend and resume wait on heartbeats.** Checkpoint requests and their
  reports still ride the heartbeat, so a suspend or resume takes one to two
  heartbeat intervals of control-plane time on top of the checkpoint itself
  (2 to 3 seconds at the default 2 s cadence). Placements do not.
- **Edge boot's speculative dispatch still uses `PostWork` pushes,** not the
  stream, and its verdict arrives on the `RatifyClaim` reply.
- **Per-sandbox lifecycle events go to the replicated event journal**, which
  keeps the newest ~10,000 rows; a platform creating thousands of sandboxes
  a second should follow `legion events follow` rather than page history.
- **One scheduler lane orders every placement,** one entry at a time, in
  both raft topologies. The lane bounds a cluster's lifecycle rate: on a
  4-vCPU reference host (one manager, simulated nodes), about 50 full
  create → suspend → resume → destroy lifecycles a second, at about 6 ms
  of lane time per entry and about 3 entries per lifecycle under load.
  Below that, a create is running in about 12 ms at p50 and 70 ms at p99;
  past it, requests queue and latency grows. More managers add replicas,
  not lanes, so they do not raise the ceiling. Measure your own hardware
  with the tools [above](#measuring-it-on-your-hardware). Unnamed
  [sessions](session-mode.md) do not use the lane to start or stop.
