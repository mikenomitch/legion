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
| c5.metal, 96 vCPU, bare metal | 48 (1,000 in 20.7 s) | under 100/s offered | about 47/s | Contention in the worker: the host was about 90% idle |

On metal a single boot takes 0.21 s (create to ready, p50), but the node
does not turn its 96 cores into more boots: starts wait on the worker's
task-manager lock (`register` up to 1.8 s) and on process spawns (`vmm_spawn`
up to 2.6 s) while the CPUs idle. That per-node ceiling of about 50
boots a second is the first thing to fix next (internal_docs/TODO.md).

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
