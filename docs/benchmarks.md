# Benchmarks

How fast does Legion place work, and how does that hold up as the fleet
grows? This page collects the numbers from our AWS benchmark rig, with
Nomad and Kubernetes run on the same hardware for comparison.

These are initial results. Every cell is a single run on simulated nodes,
so read them as a clear signal about shape and order of magnitude rather
than as a spec sheet. The rig, the scripts, and the raw archives are all in
the repository, and [Methodology](#methodology) covers what we did to keep
the comparison fair.

## Highlights

- **Placement throughput stays high from 1,000 to 20,000 nodes.** Legion
  placed 829 allocations/sec at 1k nodes and 553/sec at 20k, and completed
  a 250,000-allocation burst on a 50,000-node fleet.
- **Several times Nomad and Kubernetes on the same rig.** At 1k-2k nodes
  Legion placed 4.5-10x the allocations/sec of Nomad and Kubernetes. At 5k
  nodes it placed 17x Kubernetes, and Nomad's leader failed partway
  through the burst.
- **Millisecond latency below saturation.** Submitted at a steady rate,
  allocations land in about 20ms at the median and under 300ms at p99,
  even on a 20,000-node fleet.
- **Fast recovery.** When a quarter of a 5,000-node fleet died at once,
  Legion replaced every orphaned allocation in 27 seconds, against 111 for
  Nomad. At 10,000 nodes it replaced 6,258 allocations in under 50 seconds.

## The rig

- **Control plane:** 3 x `m6id.xlarge` (4 vCPU, 16 GiB, local NVMe). Legion
  runs as a 3-voter raft cluster, Nomad as 3 servers, and Kubernetes as a
  3-node kubeadm control plane with stacked etcd.
- **Fleet:** spot instances running each system's own node simulator:
  Legion's [`--simulate` mode](worker-simulation.md),
  [nomad-nodesim](https://github.com/hashicorp-forge/nomad-nodesim), and
  [KWOK](https://kwok.sigs.k8s.io/). About 1,250 simulated nodes per
  instance. Every simulated node speaks its system's real node protocol, so
  the control plane does the same work it would for real machines.
- **Load generator:** one `m7i.4xlarge` submitting through each system's
  native API with pooled connections.
- **Versions:** Nomad 2.0.5, Kubernetes 1.36.5 with KWOK 0.8.0, and Legion
  at the commit recorded in each cell's archive. All three run their
  default configuration, including heartbeat cadence (Legion's
  [adaptive cadence](heartbeat-cadence-and-scale.md) and Nomad's own
  rate-based scaling).

The workloads are the scheduler scenarios in
[`benchmarks/scheduler/scenarios/`](../benchmarks/scheduler/scenarios/):

- **`web_fleet`:** spread-scheduled services of about 6 allocations per
  job, with class constraints across three node profiles. Sized at 5
  allocations per node, so a 10k-node cell places 50,000 allocations.
- **`batch_burst`:** bin-packed batch jobs of 16-20 allocations each.
- **`dense_fleet`:** 50-replica services with small requests, built to
  saturate the scheduler rather than the submitter.

## Throughput as the fleet grows

Each cell submits the whole workload at once and measures placed
allocations per second over the steady window, after a 30-second warmup.

**What the p99 column means here.** Every allocation is timed from the
moment its job was submitted to the moment it was placed, and p99 is the
time within which 99% of allocations were placed. In a burst cell, every
job is submitted in the first few seconds, so the p99 allocation is one
of the last in line. Its wait is mostly time spent queued behind the rest
of the burst, which makes this column "how long it took to place 99% of
the whole burst," not how long a typical job waits. A single job on a
cluster that isn't saturated waits milliseconds (see
[Latency below saturation](#latency-below-saturation)).

| Nodes  | Allocations | Allocations/sec | p99: time to place 99% of the burst | Heartbeat interval |
| ------ | ----------- | --------------- | ----------------------------------- | ------------------ |
| 1,000  | 5,000       | **829**         | 4.2s                   | 2s                 |
| 2,000  | 10,000      | **789**         | 7.0s                   | 2s                 |
| 5,000  | 25,000      | **905**         | 18.6s                  | 2-5s               |
| 10,000 | 50,000      | **738**         | 43.6s                  | 5-8s               |
| 20,000 | 100,000     | **553**         | 99.6s                  | up to 21s          |
| 50,000 | 250,000     | **327**         | 427s                   | 34-55s             |

As a rule of thumb, the p99 is roughly `allocations / throughput`: 100,000
allocations at 553/sec take about 180 seconds to place in full, and the
p99 allocation lands near the end of that.

The heartbeat column is the interval adaptive cadence served the fleet,
which holds the control plane near 2,500 check-ins/sec however big the
fleet gets. The trade is failure detection: a node is declared lost after 5
missed intervals, so about 40s at 10k nodes and about 105s at 20k. Pin a
lower `maxInterval` on the pools where that matters, and see
[Heartbeat cadence and scale](heartbeat-cadence-and-scale.md).

At 50k nodes the plan step itself dominates (about 45ms per job against
26ms at 20k), and the leader uses about half its 4 cores. That is the next
thing to make flat.

## Compared with Nomad and Kubernetes

Same rig, same workload, each system on its own simulator and submitting
through its own API. The Nomad and Kubernetes cells come from a comparison
run against an earlier Legion build, which is shown too so you can see how
much of the gap is the recent scaling work.

**`web_fleet`, allocations/sec:**

| Nodes  | Legion (current) | Legion (comparison build) | Nomad 2.0.5    | Kubernetes 1.36.5 |
| ------ | ---------------- | ------------------------- | -------------- | ----------------- |
| 1,000  | **829**          | 733                       | 122 (182)      | 117 (100)         |
| 2,000  | **789**          | 669                       | 110 (97)       | 78 (80)           |
| 5,000  | **905**          | 736                       | failed         | 53 (45)           |
| 10,000 | **738**          | 322                       | not run        | not measured      |

Numbers in parentheses are a second run of the same cell. At 5k nodes the
Nomad leader lost leadership partway through the burst and rejected
submissions with `leadership lost while committing log`, so that cell has
no number. The Kubernetes 10k cell was invalid for a rig reason, not a
Kubernetes one: about 15,600 pods from the previous cell survived cleanup,
and the new cell's submissions collided with their names.

**`batch_burst` at 5,000 nodes:**

| System                    | Allocations/sec | p50: time to place half the burst | p99: time to place 99% of the burst |
| ------------------------- | --------------- | --------------------------------- | ----------------------------------- |
| Legion (current)          | **1,879**       | 6.2s        | 8.0s        |
| Legion (comparison build) | 1,463           | 8.4s        | 11.2s       |
| Nomad 2.0.5               | 83              | -           | 312s        |
| Kubernetes 1.36.5         | 66              | -           | 364s        |

**`dense_fleet` at 5,000 nodes, by allocations per node:**

| Allocations per node | Allocations | Legion (current) | Legion (comparison build) | Nomad 2.0.5 | Kubernetes 1.36.5 |
| -------------------- | ----------- | ---------------- | ------------------------- | ----------- | ----------------- |
| 5                    | 25,000      | **3,166**        | 2,401                     | 76          | 76                |
| 10                   | 50,000      | **2,879**        | 2,049                     | 87          | 56                |
| 20                   | 100,000     | **3,235**        | 1,801                     | 83          | failed            |
| 40                   | 200,000     | **2,797**        | 1,049                     | 88          | not run           |

Allocations/sec. Legion's time to place 99% of each burst ran from 6.5s
at 25,000 allocations to 54s at 200,000. The 10 and 20 cells' event-stream
counts ran 1.4% and 1.7% above the manager's own allocation gauge, which
the driver flags. The previous cell's cleanup was still running, which
would lower the gauge, but read those two cells as about 2% uncertain.

**Submitting through the CLI.** As a control, we also ran `web_fleet` at
5k nodes with every system submitting one CLI process per job. That caps
all three at the load generator's process-spawn rate, and the order holds:
Legion 311 allocations/sec, Nomad 59, Kubernetes 53.

## Why Legion is faster than Kubernetes

Kubernetes and Legion both keep cluster state in a replicated store and
run a scheduler against it. Most of the gap above comes from how many
writes and round trips each placement costs, and from the limits
Kubernetes ships with by default. The differences below are ranked by how
much we think each one contributes. We have not measured them one at a
time.

1. **One evaluation places a whole job.** Legion's scheduler takes a job,
   places all of its allocations in one pass, and submits them as a single
   plan. Kubernetes splits that work across a chain of controllers: the
   Deployment controller creates a ReplicaSet, the ReplicaSet controller
   creates each Pod, and the scheduler binds Pods one at a time. A
   50-replica service is one plan in Legion and more than 100 API writes in
   Kubernetes.
2. **Commits are batched, and each one is cheap.** Legion's plan applier
   folds several concurrent plans into one replicated commit, written
   straight to its own store. In Kubernetes, every Pod creation, binding,
   and status update makes its own trip through the API server. The API
   server decodes the object, runs admission and validation, encodes it
   again, writes it to etcd as its own transaction, and then serializes it
   out to every component watching that resource. Past 10,000 nodes,
   committing is most of Legion's plan time too, but a batch shares that
   cost where Kubernetes pays it per object.
3. **No client rate limits in the hot path.** Kubernetes control-plane
   components throttle their own calls to the API server. By default
   kube-scheduler allows 50 requests/sec and kube-controller-manager 20.
   That alone puts a ceiling near the 50 to 120 allocations/sec we
   measured for Kubernetes at every fleet size. Legion's scheduler writes
   to its own store and has no equivalent throttle.
4. **Planning runs in parallel against cheap snapshots.** Legion's eval
   workers each plan against an in-memory snapshot of the cluster built on
   persistent data structures, so taking one is cheap and never blocks
   writers. Several workers plan at once, and
   [late-binding plan apply](late-binding-plan-apply.md) catches conflicts
   at commit time. kube-scheduler also plans from an in-memory cache, but it
   schedules one Pod per cycle.
5. **Indexes narrow the candidate nodes.** Kubernetes filters nodes
   linearly for every Pod and, on large clusters, scores only a sample of
   them (`percentageOfNodesToScore`) to keep that scan affordable. Legion's
   [scheduling indexes](scheduling-indexes.md) ask for the nodes a task
   could land on, best fit first, and verify only those. A built-in index
   over free cpu, memory, and disk is on by default once a fleet passes 128
   nodes. On 5,000 nodes it makes a 12-task plan about 21% faster, with
   larger gains for more selective constraints.
6. **Replication can be split into lanes.** With the `lanes`
   [raft topology](multi-raft.md), Legion replicates control-plane writes,
   placements, and node status in separate raft groups, so they don't queue
   behind each other. Kubernetes sends every kind of write through one etcd
   cluster. The rig runs `lanes`, which is opt-in. The default is a single
   raft group.
7. **Heartbeat load stays flat as the fleet grows.**
   [Adaptive cadence](heartbeat-cadence-and-scale.md) stretches each
   node's check-in interval so the control plane handles about 2,500
   check-ins/sec at any fleet size. Each Kubernetes node renews a Lease
   object every 10 seconds, and those etcd writes grow with the node count.
8. **Recovery starts sooner by default.** This one is policy rather than
   architecture. Legion re-places the work from lost nodes as soon as they
   are declared lost. Kubernetes waits 300 seconds before evicting Pods
   from a NotReady node, then evicts at 0.1 nodes/sec. Both are tunable,
   but the defaults explain most of the gap in the
   [storm drill](#recovery-from-a-node-loss-storm).

These choices cost something. Kubernetes' model, where every resource is
an API object any controller can watch and act on, is a large part of what
makes it so extensible, and Legion gives some of that up. Adaptive cadence
also trades failure-detection time on very large fleets: at 20,000 nodes a
node is declared lost after about 105 seconds unless you pin a shorter
interval.

## Latency below saturation

A burst tells you capacity. To see what one job experiences, we submit at
a fixed 50 jobs/sec (about 300 allocations/sec, below capacity at every
size) and time each allocation from submit to placement.

Here p50, p95, and p99 mean what they usually do: the time within which
50%, 95%, or 99% of allocations were placed after their job was submitted.
Nothing queues behind a burst, so this is the wait a single job sees.

| Nodes  | Allocations | p50   | p95   | p99    |
| ------ | ----------- | ----- | ----- | ------ |
| 5,000  | 25,000      | 19ms  | 21ms  | 25ms   |
| 10,000 | 50,000      | 20ms  | 24ms  | 66ms   |
| 20,000 | 100,000     | 23ms  | 37ms  | 285ms  |

The median barely moves from 5k to 20k nodes. The tail grows at 20k, and
we have not yet broken down where that time goes.

## Recovery from a node-loss storm

The storm drill kills one simulator instance, about 1,250 nodes, while the
workload is resident, and times how long it takes to place every orphaned
allocation somewhere else.

**All three systems at 5,000 nodes.** The drill kills a quarter of the
fleet, with one allocation per node resident.

| System            | Allocations replaced | First replacement after | All replaced after |
| ----------------- | -------------------- | ----------------------- | ------------------ |
| Legion            | 1,267                | 20.8s                   | **27.1s**          |
| Nomad 2.0.5       | 1,270                | 13.2s                   | 110.7s             |
| Kubernetes 1.36.5 | 146 of 1,251 in 30m  | 343s                    | not within 30m     |

Nomad notices the loss sooner, and Legion finishes the replacement about 4x
faster. Kubernetes behaves as its defaults say it should. It waits 300
seconds before evicting from a NotReady node, then evicts at 0.1 nodes/sec,
so it had replaced 146 allocations when the drill's 30-minute watch ended.
At that rate the full replacement would take about 3.5 hours. Both
settings are tunable, and we ran the defaults.

**Legion at larger fleets:**

| Fleet  | Allocations replaced | First replacement after | All replaced after |
| ------ | -------------------- | ----------------------- | ------------------ |
| 10,000 | 6,258                | 33.6s                   | 49.6s              |
| 20,000 | 7,017                | 84s                     | 109.6s             |

Most of the time before the first replacement is failure detection, set by
the heartbeat interval above, which grows with the fleet. Once the nodes
are declared lost, Legion replaces the 10k-node orphans at roughly 400
allocations/sec.

## Methodology

- **Same hardware, one system at a time.** Every system runs on the same
  three control-plane boxes in turn, and the boxes are wiped between
  campaigns.
- **Simulated nodes, real protocols.** Each system's fleet is its own
  ecosystem's simulator, shaped from the same node profiles. This tests
  the control plane, which is the point, and not task runtimes.
- **Native submission, checked.** Legion submits through `legion-loadgen`,
  and Nomad and Kubernetes through pooled HTTP. Each cell records whether
  the submitter, rather than the system, set the rate, and the CLI control
  row above removes the submitter from the comparison entirely.
- **Counts are cross-checked.** The driver counts placements from the event
  stream and compares that with the manager's own allocation gauge, and
  flags disagreements.
- **Default configuration.** Every non-default setting is commented in the
  rig's configs and archived with the cell, along with versions, the binary
  hash, and instance types.

What these numbers do not cover:

- **One run per cell.** Repeated cells moved by up to half for Nomad
  and 15% for Kubernetes, so treat small differences as noise.
- **Simulated nodes.** No containers start, so these are scheduling and
  control-plane numbers, not end-to-end deploy times.
- **One region, one network.** The control plane and the fleet share a VPC.

The rig, runbook, and driver are in
[`benchmarks/aws/`](../benchmarks/aws/README.md). For the scheduler
micro-benchmarks that gate every change, see
[Testing and Benchmarks](testing-and-benchmarks.md) and
[`benchmarks/scheduler/README.md`](../benchmarks/scheduler/README.md).
