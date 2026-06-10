// Minimal slot-mode stateful job (see docs/stateful-workloads.md).
//
// Each of the 3 replicas is a stable *instance* with an ordinal slot 0..2.
// If an allocation dies or its node is lost, the replacement allocation
// reuses the same instance id (`stateful-demo-default-<slot>`) and the same
// opaque storage handle — so replica N always reattaches replica N's state.
//
// The task can observe its own identity through the injected env vars:
//   LEGION_INSTANCE_ID    stable id, survives reschedules
//   LEGION_INSTANCE_SLOT  ordinal 0..count-1 (slot mode only)
//   LEGION_INSTANCE_MODE  "slot"
//   LEGION_ALLOC_ID       the ephemeral placement id (changes each move)
{
    name: "stateful-demo",
    tasks: [
        {
            name: "replica",
            kind: "exec",
            count: 3,
            stateful: {
                // Fixed pool of ordinal slots; ids are reused within the pool
                // (replica 2 coming back must be replica 2). Use mode "keyed"
                // for on-demand, never-recycled instances (sandbox style;
                // management API is a follow-up).
                mode: "slot",
                storage: {
                    // Contract only in v1: the instance gets an opaque
                    // storage handle bound to this path. Attach/detach
                    // drivers land with the volume plugin system.
                    path: "/var/lib/replica",
                    // Keep slot storage when scaling in so a later scale-out
                    // reattaches the same data (default: true). The job-stop
                    // analog is `retainOnStop` (also default true): set it to
                    // false to drop slot handles on `job stop` so a re-run
                    // mints fresh ones (K8s whenDeleted: Delete; `job
                    // destroy` always deletes regardless).
                    retainOnScaleIn: true,
                },
            },
            config: {
                // Print who we are, then stay alive so the env can be
                // inspected (e.g. via the worker's process table).
                command: [
                    "sh",
                    "-c",
                    'echo "instance=$LEGION_INSTANCE_ID slot=$LEGION_INSTANCE_SLOT alloc=$LEGION_ALLOC_ID"; sleep 600',
                ],
            },
            resources: {
                memory: 64,
            },
        },
    ],
}
