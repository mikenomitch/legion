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
    // Whether this node serves `Worker.StartClaim` — that is, whether it will
    // start work on a boot ticket at all (docs/edge-boot.md). Off by default
    // and independent of the manager's `edgeBoot.enabled`: an operator who
    // turns the feature on cluster-wide still chooses which nodes are edges.
    // edgeBoot: {
    //     acceptClaims: false,
    //     // How often the cached JWKS is proactively refreshed. A fetch
    //     // failure never invalidates cached keys — a node cut off from its
    //     // managers must still be able to redeem tickets it can verify.
    //     jwksRefresh: "5m",
    //     // Tolerance for clock disagreement with the minting manager.
    //     clockSkew: "30s",
    // },
}
