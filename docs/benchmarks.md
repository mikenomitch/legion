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
- **Many times Nomad and Kubernetes on the same rig.** With all three
  systems' stores on the same local NVMe, Legion placed 3.5-7.5x the
  allocations/sec of Nomad and Kubernetes at 1k-2k nodes and 22-35x at
  5k-10k. Both competitors are CPU-bound on one control-plane box from 5k
  nodes up, not disk-bound.
- **Millisecond latency below saturation.** Submitted at a steady rate,
  allocations land in about 20ms at the median and under 300ms at p99,
  even on a 20,000-node fleet.
- **Fast recovery.** When a quarter of a 5,000-node fleet died with about
  6,000 allocations on it, Legion replaced every one in 37 seconds, against
  80 for Nomad. Kubernetes, on its default eviction timers, had replaced
  under 12% after 30 minutes.
- **MicroVM sandboxes ready in about 200 ms.** Session sandboxes on real KVM
  went from create to a guest that takes exec in 180 to 230 ms, and four
  4-vCPU workers booted 160 in 2.3 seconds, with no write to the replicated
  store. That rig and its projections are in [Sandboxes at
  scale](sandboxes-at-scale.md#measured-on-aws), not on this page.

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
- **Storage:** every system's store (Legion's raft log, Nomad's raft log,
  and etcd) sits on the control plane's local NVMe.
- **Versions:** Nomad 2.0.5, Kubernetes 1.36.5 with KWOK 0.8.0, and Legion
  at the commit recorded in each cell's archive. All three run their
  default heartbeat behavior (Legion's
  [adaptive cadence](heartbeat-cadence-and-scale.md), Nomad's rate-scaled
  TTL, and Kubernetes' 10-second node leases). Legion runs the `lanes`
  [raft topology](multi-raft.md), which is opt-in and recommended at this
  fleet size. Kubernetes runs with a few documented changes, listed under
  [Methodology](#methodology).

The workloads are the scheduler scenarios in
[`benchmarks/scheduler/scenarios/`](../benchmarks/scheduler/scenarios/):

- **`web_fleet`:** spread-scheduled services of about 6 allocations per
  job, with class constraints across three node profiles. Sized at 5
  allocations per node, so a 10k-node cell places 50,000 allocations.
- **`batch_burst`:** bin-packed batch jobs of 16-20 allocations each.
- **`dense_fleet`:** 50-replica services with small requests, built to
  saturate the scheduler rather than the submitter.

## Throughput as the fleet grows

These Legion-only cells ran on 2026-10-10 at commit `01d75511`, before the
cross-system rerun below, and are the only runs at 20k and 50k nodes. The
rerun's Legion numbers at 1k-10k come from a later `main` and differ by up
to 20% in either direction, which is about the single-run noise of these
cells.

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

Same rig, same workload, measured on 2026-10-14 with every system's store
on the same local NVMe. Each system runs on its own node simulator and
submits through its own API. Legion is built from `main` at `89229706`.

**`web_fleet`, allocations/sec:**

| Nodes  | Legion    | Nomad 2.0.5 | Kubernetes 1.36.5 |
| ------ | --------- | ----------- | ----------------- |
| 1,000  | **680**   | 186         | 96                |
| 2,000  | **640**   | 114         | 85                |
| 5,000  | **1,175** | 53          | 52                |
| 10,000 | **635**   | 27          | 18                |

Each number is one run, so the multiple depends on which fleet size you
quote: Legion leads by 3.5-7.5x at 1k-2k nodes and 22-35x at 5k-10k. Its
peak at 5k is not a single outlier. The fixed-heartbeat ablation below
reran that cell at 1,134/sec, and the earlier build also peaked at 5k
(905/sec, against 738 at 10k).

Nomad's 1k-5k cells are a rerun with Prometheus scraping every 15 seconds.
At 5 seconds the scrape cost its leader enough CPU to read 188, 102, and
49 allocations/sec.

The Kubernetes 10k cell reached the driver's 42-minute cap with 92% of its
50,000 pods placed, so its number is the rate over that window. Nomad
rejected some submissions with a 5xx at 5k and 10k nodes, while its leader
briefly lost leadership (`leadership lost while committing log`), and the
driver retried them. Every retry counts against Nomad's latency, not its
allocation count.

**Why the competitors run at these rates.** An earlier run of this
comparison kept Nomad's raft log and etcd on network storage (gp3) while
Legion's was on local NVMe. Moving them to NVMe changed neither system's
throughput beyond run-to-run noise, because neither is waiting on its
disk. Both are out of CPU on one control-plane box:

- **Nomad's leader** runs at 3.2-3.3 of its 4 cores from 5k nodes up, with
  disk wait near zero. Part of that is heartbeats. Nomad 2.0.5's client
  sends a heartbeat after nearly every successful RPC, so the leader served
  2,500-3,300 heartbeats/sec at 2k-5k nodes, far more than its 50/sec TTL
  target suggests. That is Nomad's own client code running inside the
  simulator, so we measured it as shipped.
- **Kubernetes** is limited by kube-scheduler's one-Pod-at-a-time cycle.
  At 5k nodes the controllers create Pods at about 150/sec, but the
  scheduler binds 40-70/sec while 10,000-15,000 Pods wait in its queue. A
  scheduling attempt averages 31ms, and the largest share is PreScore,
  mostly the default `PodTopologySpread` constraints. The box running the
  scheduler, controller manager, an API server, and an etcd member sits at
  3.4-3.5 of 4 cores.

**`dense_fleet` at 5,000 nodes, by allocations per node:**

| Allocations per node | Allocations | Legion       | Nomad 2.0.5 | Kubernetes 1.36.5 |
| -------------------- | ----------- | ------------ | ----------- | ----------------- |
| 5                    | 25,000      | pending  | 78          | 69                |
| 10                   | 50,000      | pending | 78    | 46                |
| 20                   | 100,000     | pending | pending    | 25                |
| 40                   | 200,000     | pending | pending    | 30, then failed   |

At 40 allocations per node, Kubernetes placed about 12,000 Pods at
30/sec over six minutes. Then the API server on the box running the
scheduler stopped completing new TLS handshakes in time, and the driver's
Pod watch could not reconnect. The cell has no full result.

**`batch_burst` at 5,000 nodes:**

| System            | Allocations/sec | p99: time to place 99% of the burst |
| ----------------- | --------------- | ----------------------------------- |
| Legion            | pending      | pending                           |
| Nomad 2.0.5       | 77              | 331s                                |
| Kubernetes 1.36.5 | pending          | pending                           |

**What two changes are worth.** We ran two of the setup choices both ways
on `web_fleet`:

- **Kubernetes with every simulated node talking to one API server.** In
  production an HA control plane's API servers sit behind a load balancer.
  The rig spreads the simulated nodes across all three. With all of them
  on one, Kubernetes placed 43.7 Pods/sec at 5k nodes, against 52.2 with
  the spread.
- **Legion with fixed 2-second heartbeats** instead of adaptive cadence:
  1,134 allocations/sec at 5k nodes (adaptive: 1,175) and 519 at 10k
  (adaptive: 635), while serving about 2,700 heartbeats/sec at 10k. Adaptive
  cadence is worth up to about 18% of Legion's throughput at 10k, far less
  than the gap to either competitor.

**Submitting through the CLI.** As a control in the earlier run, we also
ran `web_fleet` at 5k nodes with every system submitting one CLI process
per job. That caps all three at the load generator's process-spawn rate,
and the order held: Legion 311 allocations/sec, Nomad 59, Kubernetes 53.

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
3. **No per-object scheduling cycle.** kube-scheduler runs its filter and
   score plugins once per Pod, one Pod at a time, and its API clients are
   rate-limited by default (50 requests/sec for the scheduler, 20 for the
   controller manager). The rig raises those limits to 1,000, so they are
   not what caps Kubernetes here: the scheduling cycle is, at about 31ms
   per Pod on 5,000 nodes. Legion plans a whole job per evaluation and
   writes to its own store with no client throttle.
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
   object every 10 seconds, and those etcd writes grow with the node count:
   about 600/sec at 5,000 nodes on the rig.
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
fleet with 5 allocations per node resident, about 6,000 to replace.

| System            | Allocations replaced  | First replacement after | All replaced after |
| ----------------- | --------------------- | ----------------------- | ------------------ |
| Legion            | 6,173                 | 20.9s                   | **36.5s**          |
| Nomad 2.0.5       | 6,106                 | 10.9s                   | 80.3s (95.1s in a second run) |
| Kubernetes 1.36.5 | 697 of 6,248 in 30m   | 345s                    | not within 30m     |

Nomad notices the loss sooner, and Legion finishes the replacement about
2x faster. Kubernetes behaves as its defaults say it should. It waits 300
seconds before evicting from a NotReady node, then evicts at 0.1
nodes/sec, so it had replaced 697 allocations when the drill's 30-minute
watch ended. Both settings are tunable, and we ran the defaults.

**Legion at larger fleets** (2026-10-10, one allocation per node
resident):

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
- **Same disk.** Every system's store sits on the boxes' local NVMe. Each
  cell records CPU time by mode and disk busy time, so it shows whether a
  system was waiting on its disk or out of CPU.
- **The measurement does not tax the measured.** Prometheus scrapes each
  control plane every 15 seconds. At 5 seconds, scraping Nomad's leader
  (about 49,000 samples at 4,000 jobs) cost it a quarter of a core.
- **Simulated nodes, real protocols.** Each system's fleet is its own
  ecosystem's simulator, shaped from the same node profiles. This tests
  the control plane, which is the point, and not task runtimes.
- **Native submission, checked.** Legion submits through `legion-loadgen`,
  and Nomad and Kubernetes through pooled HTTP. Each cell records whether
  the submitter, rather than the system, set the rate, and the CLI control
  row above removes the submitter from the comparison entirely.
- **Counts are cross-checked.** The driver counts placements from each
  system's event stream and compares that with the system's own allocation
  count, and flags disagreements. When the stream falls a few events short
  but the system's count shows every allocation placed, the cell completes
  and records the shortfall.
- **Default configuration, with documented exceptions.** Kubernetes runs
  with its scheduler's and controller manager's API rate limits raised to
  1,000/sec, 50 sync workers for ReplicaSets and Deployments, and an 8 GiB
  etcd quota, and its simulated nodes spread across the three API servers.
  Nomad's per-client connection caps are lifted because about 1,250
  simulated clients share each worker's IP. Every non-default setting is
  commented in the rig's configs and archived with the cell, along with
  versions, the binary hash, and instance types.

What these numbers do not cover:

- **One run per cell.** Repeated cells moved by up to half for Nomad
  and 15-20% for Kubernetes and Legion, so treat small differences as
  noise.
- **Simulated nodes.** No containers start, so these are scheduling and
  control-plane numbers, not end-to-end deploy times.
- **One region, one network.** The control plane and the fleet share a VPC.

The rig, runbook, and driver are in
[`benchmarks/aws/`](../benchmarks/aws/README.md). For the scheduler
micro-benchmarks that gate every change, see
[Testing and Benchmarks](testing-and-benchmarks.md) and
[`benchmarks/scheduler/README.md`](../benchmarks/scheduler/README.md).
