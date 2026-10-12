# A sandbox whose suspend is published to the workers' content store
# (suspend.durability = "store"), so it can resume on the other worker. For
# RUNBOOK 12.6 and smoke.sh's sandbox-store scenario.
#
#   legion job run tests/spot-check/jobs/10-sandbox-store.ncl
#   legion instance create sandbox-store --id sbs-1
#   legion instance suspend sbs-1 --wait -j          # durability: store
#   legion instance get sbs-1 -j | jq -r .data.instance.checkpoint_state  # local, then published
#   legion node cordon <its node>                    # force the resume elsewhere
#   legion instance start sbs-1 --wait -j            # fetches the parts, restores
#
# No stateful.storage: a published checkpoint can only move if the instance's
# data is not pinned to one node, and the rig has no network volume driver.
# Everything the guest holds (its root disk, /tmp, its memory) rides in the
# checkpoint (docs/suspend-and-resume.md, "The volume has to move too").
#
# Needs a `contentStore` in the rig's worker config and a credential for it in
# KV (RUNBOOK 12.0). Without one, `instance suspend` is refused with
# FailedPrecondition naming the node and the block to add, and the sandbox
# keeps running.
let images = (import "lib/images.ncl") vars in
{
  name = "sandbox-store",
  task = {
    name = "scratch",
    runtime = "sandbox",
    stateful = { mode = "keyed" },
    config = {
      image = images.agent,
      command = [
        "/bin/sh",
        "-c",
        "echo \"sandbox $LEGION_INSTANCE_ID booted on $(hostname)\"; exec sleep 86400",
      ],
      vcpus = 1,
      hypervisor = "cloud-hypervisor",
    },
    resources = { cpu = 250, memory = 256 },
    suspend = {
      checkpoint = "memory",
      durability = "store",
      onRestoreFailure = "cold",
    },
    constraints = { os = "linux" },
  },
}
