# Edge boot: microVM sandboxes that start before the placement is durable
# (docs/edge-boot.md).
#
# This is the shape a sandbox platform wants. Each user's sandbox is a keyed
# instance created on demand; a request arriving at an edge node starts it
# there immediately, with the guest's network held shut, while the manager
# commits the placement in parallel. If two regions race for the same user,
# exactly one is confirmed and the loser kills its VM before it ever had a
# network.
#
#   legion job run configs/jobs/sandbox-edge-boot.ncl
#   legion instance create edge-sandboxes --id user-42
#
#   # Ahead of time, from the platform's control plane:
#   legion boot-ticket create edge-sandboxes \
#       --task workspace --id user-42 --pool edge-eu --gate boot --ttl 5m
#
#   # On the request path, at the edge — no manager call:
#   legion sandbox start edge-sandboxes \
#       --id user-42 --node http://10.0.1.7:4848 --ticket "$TICKET"
#
# Requires `edgeBoot.enabled = true` on the manager (plus
# `workloadIdentity.enabled`, which signs the tickets) and
# `edgeBoot.acceptClaims: true` on the edge workers. All three are off by
# default — see docs/edge-boot.md "Enabling it".
{
    name = "edge-sandboxes",
    tasks = [
        {
            name = "workspace",
            runtime = "sandbox",
            group = "sandboxes",
            stateful = {
                # On-demand instances, never-recycled ids: a destroyed
                # sandbox's identity (and its storage) can never leak into a
                # new one.
                mode = "keyed",
                storage = {
                    path = "/workspace",
                    mode = "block",
                    params = { size = "1G" },
                    # The default `local-dir` driver is node-local, which is
                    # what keeps this task eligible for gates above `stage`:
                    # the only possible racer for the volume is on the same
                    # node, where the worker's claim table already fences it.
                    # A shared driver here would downgrade the gate to
                    # `stage` at admission, with a warning at `job validate`.
                },
            },
            # The point of this jobspec.
            speculativeStart = {
                # Boot the guest immediately, with its network held shut, and
                # open it only when the placement commits. A sandbox that
                # loses its race is killed before it ever reached the
                # outside world.
                mode = "boot",
                # Long enough to ride out a raft election, short enough that
                # a partitioned node's wasted VM is bounded.
                ratifyTimeout = "5s",
                # A loser is destroyed outright: the common refusal is
                # "someone else owns this sandbox", and a loser lingering is
                # exactly what the at-most-one invariant forbids.
                onRefused = "kill",
            },
            config = {
                image = "alpine:latest",
                command = [
                    "/bin/sh",
                    "-c",
                    # A provisional start has no allocation id yet — it does
                    # not exist until the commit — so the workload is given
                    # LEGION_CLAIM_ID and the instance id it was started for,
                    # which is stable from the first millisecond.
                    "echo \"sandbox=$LEGION_INSTANCE_ID claim=$LEGION_CLAIM_ID\"; " ++
                    "echo boot >> /workspace/boots.log; " ++
                    "sleep 3600",
                ],
                vcpus = 2,
                rootfs_headroom_mib = 256,
                # The `boot` gate is a REAL network boundary only for this
                # runtime, and only through a TAP device Legion can hold
                # down. Uncomment on a host with a bridge and
                # CAP_NET_ADMIN; with the default loopback-only guest there
                # is no network to gate, which is strictly stronger.
                # network = { mode = "tap", tap = "auto", bridge = "br0" },
            },
            resources = {
                cpu = 2000,
                memory = 1024,
            },
            # Edge nodes live in their own pool; the ticket names the same
            # pool, and the worker checks it offline against its own
            # registration before it will start anything.
            permit = {
                pool = "edge-eu",
            },
            constraints = {
                os = "linux",
            },
        },
    ],
}
