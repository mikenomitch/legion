# Sandbox-boot rig

A small real cluster on AWS for measuring how fast Legion starts sandbox
sessions on real KVM: how long one microVM takes from `CreateInstance` to a
guest that takes exec, and how many a node and a cluster boot per second,
in a burst and sustained. The results and the projections built on them are
in [`docs/sandboxes-at-scale.md`](../../../docs/sandboxes-at-scale.md#measured-on-aws).

Every box is a spot `c8i` instance with
[nested virtualization](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html),
so cloud-hypervisor runs without bare metal. Nested virtualization makes
each VM exit costlier than on metal, so treat the numbers as a floor for the
same core count on metal.

| Box | Type | Pricing | Runs |
| --- | --- | --- | --- |
| `sbx-mgr-1` | c8i.large | on-demand | The manager (raft, ACLs on). On-demand because a reclaim takes the cluster's state with it |
| `sbx-wkr-N` | c8i.xlarge (4 vCPU, 8 GiB) | spot | Workers: cloud-hypervisor v51.2 and its reference kernel |
| `sbx-big-1` | c8i.4xlarge (16 vCPU, 32 GiB) | spot | Optional: one larger worker, to see how a node scales with cores |
| `sbx-lg-1` | c8i.xlarge | spot | `legion-loadgen` |
| `sbx-build` | c8i.8xlarge | spot, persistent | Builds the Linux binaries. Stop it between builds: it is most of the bill |

Everything is tagged `Project=legion-sbx-bench` in the `legion-bench` AWS
profile, and `down.sh` deletes exactly that.

## Running it

```bash
cd benchmarks/aws/sandbox-boot
./up.sh --build                                   # boxes, key, security group, bucket
./build.sh all                                    # sync, build, push, deploy this checkout
./install.sh                                      # form the cluster
./xfs.sh                                          # optional: worker state on reflink XFS
./use-job.sh jobs/sessions-vm.ncl                 # one benchmark job's envelopes on every worker
./run.sh seq bench-vm vm --wait running --count 10 --concurrency 1
./run.sh burst bench-vm vm --wait running --count 160 --concurrency 32
./run.sh sustained bench-vm vm --wait running --count 1200 --rate 40 --concurrency 512 --hold-ms 2000
./build.sh stop                                   # between builds
./cost.sh                                         # what it has cost so far
./down.sh                                         # everything, when done
```

The shape of the rig (which workers and load boxes exist) lives in
`.state/topology`, so a run after a spot reclaim can drop the lost boxes:

```bash
SB_WORKERS="sbx-wkr-1 sbx-wkr-2" SB_LOADERS="sbx-lg-1" ./up.sh
```

## What a run measures

`run.sh` clears the job's sessions, runs `legion-loadgen --mode sessions`
(`benchmarks/loadgen/sessions.rs`) from the load box, and merges the
loadgen's summary with each worker's timeline into
`.state/results/<name>.json`:

- `loadgen.latency_ms`: the client's view. With `--wait running`, from the
  create (or from when it was due, at an open-loop `--rate`) until the
  session reports `running`, which for a sandbox means its guest agent
  takes exec.
- `guests.to_up_ms`: the node's view, from the worker receiving the start to
  the guest's agent accepting a connection on vsock port 1100, stamped on
  the host by `vsockwatch.py`. The guest's own clock cannot be used: its
  uptime restarts on a reboot and misses time the VMM spends before boot.
- `guests.guests_up_per_sec`: started sessions over the window from the
  first start to the last guest up.
- The worker log's `Task allocation started` line breaks each start into
  stages (`image`, `root_disk`, `vmm_spawn`, `ready`, `register`).

The benchmark jobs in `jobs/`:

| Job | What it is |
| --- | --- |
| `sessions-vm.ncl` | The reference sandbox: Alpine from ECR Public, 1 vCPU, 128 MiB, no NIC. 48 per c8i.xlarge |
| `sessions-vm-pinned.ncl` | The same image pinned by digest, so no tag is resolved per start |
| `sessions-vm-quiet.ncl` | `kernel_args = "quiet"`, from investigating the serial console's cost |
| `sessions-vm-big.ncl` | A 24 GiB envelope that fits only `sbx-big-1` |
| `sessions-exec.ncl`, `sessions-exec-fast.ncl` | Host processes (`exec`), to measure the control plane without a VMM. The second with `stateful.placement = "fast"` |
