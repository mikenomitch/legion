{
    workerType: "default",
    pool: "default",
    additionalNodeChecks: [],
    attributes: {
        foo: "bar"
    },
    // Heartbeat cadence in seconds (default 2, clamped to 1-30; ±10% jitter
    // per beat). The manager marks nodes lost after 10s without a heartbeat,
    // so values above 3 trade detection margin for manager load — the load
    // knob for very large fleets (docs/architecture-managers-workers.md).
    // heartbeatSeconds: 2,
    // Artifact fetch settings for workers of this type (docs/artifacts.md).
    // The values below are the defaults; the block can be omitted entirely.
    artifacts: {
        fetch: {
            // Concurrent fetches per worker.
            parallelism: 4,
            // Per-artifact retries before the allocation fails.
            attempts: 3,
            // Overall fetch deadline per artifact.
            timeout: "10m",
            // Per-artifact size cap; also bounds decompression.
            maxArtifactSize: "50GiB",
        },
    },
    // Turn a task's declared `resources` into real kernel limits for `exec`
    // tasks (docs/task-supervision.md). Off by default: a declaration that was
    // only ever a scheduling estimate should not start OOM-killing workloads
    // because the cluster upgraded. Linux + cgroup v2 only; a node that cannot
    // enforce logs a warning and runs the task unconfined.
    resourceEnforcement: {
        enabled: false,
        // Path under /sys/fs/cgroup. Must exist, with `+memory` and `+cpu` in
        // its cgroup.subtree_control; Legion creates one subtree per
        // allocation beneath it.
        cgroupParent: "legion",
    },
}
