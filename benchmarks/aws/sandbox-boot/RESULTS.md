# Sandbox-boot results, 2026-10-14

Measured with this rig ([README](README.md)) on `origin/main` (89229706)
and on `perf/sandbox-scale`. The public write-up, with projections and the
comparison with other systems, is in
[`docs/sandboxes-at-scale.md`](../../../docs/sandboxes-at-scale.md#measured-on-aws).
Total AWS cost for the campaign, builds and CI runs included: about $8.10.

**Setup.** One manager (c8i.large, 2 vCPU). Workers are c8i.xlarge (4 vCPU,
8 GiB) with nested virtualization unless stated otherwise. cloud-hypervisor
v51.2 with its reference 48 MB kernel. The sandbox is
`public.ecr.aws/docker/library/alpine:3.20`, 1 vCPU, 128 MiB, no NIC, whose
command prints one line and sleeps (`jobs/sessions-vm.ncl`). The loadgen
runs on its own c8i.xlarge.

**What "ready" means.** From the worker receiving the start to the guest's
agent accepting exec on vsock, stamped on the host (`vsockwatch.py`). On
`origin/main` a session reports `running` when its VMM spawns, hundreds of
milliseconds to seconds before that, so the before column must be read
from the host's stamps, not from the client.

## Before and after, four c8i.xlarge workers

| | `origin/main` | `perf/sandbox-scale` | |
| --- | --- | --- | --- |
| One boot, create to ready, p50 | 2.92 s | 0.229 s (0.179 s, image pinned by digest, XFS) | 13 to 16x |
| Burst of 160, time until all ready | 98 s | 2.33 s | 42x |
| Burst of 160, guests ready per second | 1.6 | 68.7 | 43x |
| Burst of 160, ready p99 | 76 s | 0.74 s | |
| Sustained, offered 20/s (2 s sessions) | 3.1/s delivered, 69% refused | 20/s, none refused, p99 0.40 s (two workers) | |
| Sustained, highest rate with nothing refused | under 20/s | 40/s, p99 0.45 s | at least 13x |
| Sustained ceiling | | about 56/s (2.5% refused at 60/s offered) | |

## Per node

| Node | Burst (guests ready/s) | Sustained, none refused | Ceiling | Bound by |
| --- | --- | --- | --- | --- |
| c8i.xlarge, 4 vCPU, nested | about 17 | about 10/s | about 14/s | CPU (guest boot and KVM exits) |
| c8i.4xlarge, 16 vCPU, nested | 54 | 40/s | about 48/s | CPU, 92% busy: 53% guest, 26% host kernel |
| c5.metal, 96 vCPU, bare metal | 246 (state on EBS), 317 to 342 (state in RAM) | 150/s (EBS), 200/s (RAM) | about 175/s (EBS), 205 to 245/s (RAM) | Root disks: EBS IOPS, or the per-sandbox clone and file write |

The c5.metal row is the second metal campaign below. The first, on an
earlier build of this branch, measured about 48 a second with the CPUs about
90% idle, with starts waiting on the worker's task-manager lock and on
process spawns.

## Bare metal, second campaign (2026-10-14)

One c5.metal worker (96 vCPU, 192 GiB, us-east-1f, spot), the manager on a
c8i.large, and the loadgen on the worker itself (no c8i.xlarge capacity in
the AZ). `jobs/sessions-vm-metal.ncl`, edge starts (`--via edge`), 2 s
sessions for the sustained runs, `edgeBoot.launchConcurrency = 384`. "EBS"
is worker state on XFS in a loop file on the gp3 root volume (6,000 IOPS,
`xfs.sh`). "RAM" is the same XFS image copied to tmpfs, standing in for
local NVMe.

Current `main` (PR #43) and with the worker lock fixes, state in RAM:

| | `main` | lock fixes |
| --- | --- | --- |
| One boot, p50 | 187 ms | 193 ms |
| Burst of 1,200 (concurrency 384) | 317/s, p99 1.94 s | 342/s, p99 1.51 s |
| 200/s offered | 198/s, none refused | 181/s, none refused |
| 300/s offered | 207/s, 591 refused | 204/s, 912 refused |
| 400/s offered | 180/s, 2,266 refused | 163/s, 2,807 refused |

`main` with state on EBS: a burst of 1,200 at 246/s, 150/s sustained with
none refused, about 175/s at the ceiling.

What the profiles showed:

- **The task-manager lock was busy 60 to 80% of the time** under 250/s with
  churn. Most of it was cgroup work (`assign` rewrote the share slice's
  `cpuset.cpus` on every start, which makes the kernel update every cpuset
  under it, and created the leaf under the lock) and the supervisor
  destroying stopped VMMs under the lock. The lock fixes cut it to about
  20%. They tightened the burst but did not raise the sustained ceiling, so
  the lock was not what bound it.
- **On EBS, the node runs out of IOPS.** The gp3 volume ran 6,500 to 6,900
  writes a second and the loop device was 99% busy: `debugfs` (which writes
  each sandbox's files into its root disk and fsyncs on close), the guests'
  own writes, and XFS reflink and inode GC. `debugfs` was in D state in
  most samples.
- **In RAM, starts wait in the root disk step:** `root_disk` p50 1.3 s at
  300/s offered, against 3 ms alone. Off-CPU, the worker waits on `debugfs`,
  and `debugfs` waits on writeback, XFS buffer locks, and inode locks:
  every clone shares extents with one base file, so their copy-on-write
  allocations meet in the same locks. Running `debugfs` without fsync
  (`eatmydata`) did not raise the ceiling. A quarter of the CPU stayed idle.
- **Ruled out:** the client's HTTP/2 connection (two loadgens on two
  connections got the same total), tokio's blocking pool (raising it from
  512 to 8,192 threads changed nothing), and cgroup migration
  (`favordynmods` removed the shim's waits but slowed guest boots).

## Ratification on three managers (2026-10-14)

Three raft managers (one c8i.large and two c7i.large) and three workers (one
c5.metal and two m6i.large), all in us-east-1f. The job is
`jobs/race-exec.ncl`: a keyed host process on the `exec` runtime with
`speculativeStart = { mode = "serve" }`. `race.sh` mints one prefix ticket
and, for every instance id, fires one `legion sandbox start` at each of the
three workers at the same moment, 20 ids at a time. It then counts the
starts that ratified per id and the processes still running per worker.

| Ids | One ratified start | More than one | None | Refusals |
| ---: | ---: | ---: | ---: | --- |
| 60 | 60 | 0 | 0 | two `instance_claimed` per id |
| 200 | 200 | 0 | 0 | two `instance_claimed` per id |
| 300, leader stopped 30 s in | 300 | 0 | 0 | two `instance_claimed` per id |

- In the 300-id run, the raft leader was stopped 30 seconds in and a new
  leader was elected (term 17). The run ended with exactly 300 processes
  across the workers, one per instance id, checked by reading each
  process's `LEGION_INSTANCE_ID`. All 300 were still running 80 seconds
  later.
- No losing start's process survived in any run: 1,120 losers across 560
  ids, none alive.

The race found four bugs, each fixed before the runs above:

- **Every ticket was refused.** The dispatch payload left out the task's
  `speculativeStart` block, so every node's spec cache read every task as
  `off`: `task box does not permit speculative starts`. The in-process
  tests fill the cache by hand, so they never caught it.
- **Most edge starts were refused after boot.** A follower answered
  `RatifyClaim` with an `internal` refusal, and the node killed the claim.
  A follower now answers not-leader, and the node retries the leader.
- **Confirmed winners were stopped.** A heartbeat answered by a follower
  that had not applied the ratification yet, or computed just before it,
  omitted the claim, and the worker stopped it: 378 ratified claims on one
  worker, with cause `unknown`. A ratified claim now stays exempt from the
  stop sweep until a desired set lists it, or for 30 seconds.
- **Workloads had the wrong identity.** An edge start's env came from the
  cached task config, whose `LEGION_INSTANCE_ID` named the instance of
  whatever allocation filled the cache.

The public write-up is in
[`docs/edge-boot.md`](../../../docs/edge-boot.md#ratification-on-a-raft-cluster).

## Console logging under a burst (2026-10-14)

On the c5.metal worker, a burst of 1,200 sandbox starts made journald fall
behind. Console log writes were blocking, so every thread that logged
waited on stdout, heartbeats included. The worker sent no heartbeat for
about 57 seconds and was marked lost, and the burst fell to 8 starts a
second. With the console behind a lossy non-blocking writer, the same burst
ran at 293 starts a second and the worker was never marked lost
([`docs/observability.md`](../../../docs/observability.md#logging-and-tracing-setup)).

## Memory restore (2026-10-14)

cloud-hypervisor v53.0, `jobs/restore-vm-lazy.ncl` and
`jobs/restore-vm-eager.ncl`: a keyed sandbox with 2 vCPUs and 2 GiB
whose command fills 900 MiB of `/dev/shm` with random data. Its memory
checkpoint is 2.4 GB.

| | This branch | `main`'s worker |
| --- | --- | --- |
| Suspend | 22 to 24 s, guest paused 0.6 to 2.7 s | |
| `restore_ms`, `lazy` | about 5,040 | |
| `restore_ms`, `eager` | about 5,040 | |
| Resume, end to end | 5.0 s | 15.6 s |

`lazy` and `eager` measure the same because a 5 second wait dominates both.
Before it resumes a restored guest, the node waits up to `DATAPATH_WAIT`
(5 s) for the datapath's egress proxy socket. Every sandbox checkpoint
records gated networking, since every guest has a vsock, so a guest that is
not gated never gets the socket and waits the full 5 seconds. That is
pre-existing on `main`, and until it is fixed this rig cannot tell lazy
from eager.

## One boot, by stage

Worker log `Task allocation started`, sequential starts on a c8i.xlarge:

| Stage | `origin/main` | After | Notes |
| --- | --- | --- | --- |
| `image` | 110 ms | 8 ms (digest), 12 to 60 ms (tag) | A tag is resolved per start: now one round trip on a kept-alive connection |
| `root_disk` | 80 ms (extract and `mkfs`) | 10 ms XFS, 60 to 80 ms ext4 | A reflink (or sparse copy) of the node's base disk plus `debugfs` |
| `vmm_spawn` | 10 ms | 10 ms | |
| guest to ready | about 2.7 s | about 140 ms | Every boot panicked, slept 1 s, and rebooted. The serial console cost about 0.7 s more |

## Control plane

| | `origin/main` | After |
| --- | --- | --- |
| Router, in process, 16 fake nodes, 64 in flight (laptop) | 19,200 starts/s, p99 4.4 ms | 36,200 starts/s, p99 2.3 ms |
| Exec sessions through one 2-vCPU manager, 4 real nodes, 64 in flight | 1,211/s, p90 154 ms | 1,316/s, p90 104 ms |
| The same, 256 in flight | 1,043/s, p90 604 ms | 1,265/s, p90 285 ms |
| Exec sessions at the edge, 4 nodes (no manager) | 1,144/s | |
| A router pick, `spread`, 1,000 / 10,000 / 100,000 nodes | | 31 µs / 330 µs / 5.2 ms |
| A router pick, `fast`, any of those | | 0.1 to 0.5 µs |
| Manager memory per running session (session index) | | about 1.2 KB: 1.2 GB per million |
| Manager CPU per heartbeat delta | | about 8 µs: 125,000 beats/s on one core |

The exec runs are bound by the nodes (about 330 starts/s each), not the
manager, which ran at about 30% of one core.

## Density

190 idle Alpine guests (128 MiB each) on the c8i.4xlarge took 13.2 GB, about
70 MiB per guest: a guest's RSS is the memory it touched, not its size. An
envelope admits sessions by declared size, so a node fits about 1.8x more
of these guests than its envelope admits.
