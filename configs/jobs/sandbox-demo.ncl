# Keyed-mode stateful job (sandbox / dispatch style; see
# docs/stateful-workloads.md).
#
# Unlike slot mode, keyed tasks define no `count` (doing so is a parse-time
# error). Submitting the job runs ZERO sandboxes; instances are created and
# destroyed on demand:
#
#   # create a sandbox with an explicit key (or omit --id for a minted one)
#   legion instance create sandbox-demo --id user-42
#
#   # inspect
#   legion instance list sandbox-demo
#   legion instance get user-42 -j
#
#   # destroy: stops the allocation, drops its storage handle, and
#   # tombstones the id FOREVER — `user-42` can never be created again
#   legion instance destroy user-42
#
# Each instance is a stable identity: if its allocation dies, the
# replacement re-binds the same instance id and storage handle. The task
# observes its identity through injected env vars:
#   LEGION_INSTANCE_ID    the keyed id, survives reschedules
#   LEGION_INSTANCE_MODE  "keyed"
#   LEGION_ALLOC_ID       the ephemeral placement id (changes each move)
# (LEGION_INSTANCE_SLOT is slot-mode-only and absent here.)
{
    name = "sandbox-demo",
    tasks = [
        {
            name = "sandbox",
            runtime = "exec",
            group = "sandboxes",
            stateful = {
                # On-demand instances with never-recycled ids. A destroyed
                # sandbox's identity and state can never leak into a new one.
                mode = "keyed",
                storage = {
                    # Contract only in v1: each instance gets an opaque
                    # storage handle bound to this path, reattached on every
                    # reschedule and dropped on destroy. Attach/detach
                    # drivers land with the volume plugin system.
                    path = "/workspace",
                },
            },
            config = {
                command = [
                    "sh",
                    "-c",
                    "echo \"sandbox=$LEGION_INSTANCE_ID mode=$LEGION_INSTANCE_MODE alloc=$LEGION_ALLOC_ID\"; sleep 600",
                ],
            },
            resources = {
                memory = 64,
            },
        },
    ],
}
