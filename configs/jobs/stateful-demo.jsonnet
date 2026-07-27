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
    // Ordinal-aware rollouts for the slot pool (see docs/stateful-workloads.md
    // "Ordered rollouts for slot pools"). Uncomment to make config updates
    // replace ONE slot at a time, highest first (2 -> 1 -> 0), each step
    // gated on the previous replacement becoming healthy (K8s OrderedReady
    // analog). Add updateFrom to stage the update: e.g. updateFrom: 2
    // updates only slot 2 and leaves slots 0-1 on the old config until a
    // resubmit lowers the floor (K8s partition analog, the canary-promote
    // workflow).
    // rollout: {
    //     slots: {
    //         order: "sequential",
    //         // updateFrom: 2,
    //     },
    // },
    tasks: [
        {
            name: "replica",
            runtime: "exec",
            count: 3,
            stateful: {
                // Fixed pool of ordinal slots; ids are reused within the pool
                // (replica 2 coming back must be replica 2). Use mode "keyed"
                // for on-demand, never-recycled instances (sandbox style;
                // management API is a follow-up).
                mode: "slot",
                // Node-loss fencing (at-most-one; see docs/stateful-workloads.md
                // "Node loss and fencing"). Optional — the values below are the
                // defaults that apply even without the block. Two cooperating
                // timers: the worker kills this task after losing manager
                // contact for selfTerminateAfter, and the manager only replaces
                // an allocation on a lost node after replaceAfter
                // (> selfTerminateAfter), so a replacement can never overlap
                // the original. Lower selfTerminateAfter for faster failover;
                // set replaceAfter to "never" for K8s StatefulSet-style manual
                // recovery (node return or `legion instance replace <id>`).
                // onLost: {
                //     selfTerminateAfter: "90s",
                //     replaceAfter: "3m",  // default 2x selfTerminateAfter; or "never"
                // },
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
