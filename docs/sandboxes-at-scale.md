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
| Destroy | 1 | the allocation removal and the tombstone, in one write |
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

- **Every sandbox is a row, and a destroyed one leaves a tombstone** for the
  life of its job, so the store grows with history, not with the running
  set. A pending instance row is about 216 bytes (a suspended one about
  1.7 KB with its checkpoint record); 10^6 instances is a ~217 MB store
  image, which every new manager and lagging follower receives. Destroying
  the job clears its history.
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
  with the tools [above](#measuring-it-on-your-hardware).
