{
    workerType = "default",
    pool = "default",
    additionalNodeChecks = [],
    attributes = {
        foo = "bar"
    },
    # Heartbeat cadence in seconds (default 2, clamped to 1-30; ±10% jitter
    # per beat). The manager marks nodes lost after 10s without a heartbeat,
    # so values above 3 trade detection margin for manager load — the load
    # knob for very large fleets (docs/architecture-managers-workers.md).
    # heartbeatSeconds = 2,
    # Artifact fetch settings for workers of this type (docs/artifacts.md).
    # The values below are the defaults; the block can be omitted entirely.
    artifacts = {
        fetch = {
            # Concurrent fetches per worker.
            parallelism = 4,
            # Per-artifact retries before the allocation fails.
            attempts = 3,
            # Overall fetch deadline per artifact.
            timeout = "10m",
            # Per-artifact size cap; also bounds decompression.
            maxArtifactSize = "50GiB",
        },
    },
    # The node content store (docs/artifacts.md, "The node content store"):
    # a content-addressed cache under <worker state dir>/content/. Artifacts
    # with a `checksum` are served from it on every allocation after the
    # first. Each class has its own budget, so checkpoint parts cannot evict
    # cached artifacts. The values below are the defaults.
    content = {
        # false stages every artifact straight from origin.
        enabled = true,
        budgets = {
            artifacts = "20GiB",
            snapshots = "50GiB",
        },
        # Unpinned content unused this long is removed. "0s" disables expiry.
        ttl = "72h",
        # Evict unpinned content, then refuse new content, before the disk
        # drops below this.
        minFreeDisk = "5GiB",
    },
    # Where this worker publishes content by digest (docs/artifacts.md,
    # "Publishing content"). Absent by default: the node publishes nothing.
    # Content lands at <url>/sha256/<hex>, so publishing is idempotent, and
    # any worker that can read the URL fetches it back through the ordinary
    # s3:// or file:// artifact fetcher, verified by digest. Point each
    # region's workers at their own bucket for region locality. A worker
    # with a contentStore publishes durable checkpoints
    # (suspend.durability = "store", `legion snapshot create --publish`) and
    # advertises the `content-publish` capability. Without one, a store
    # suspend on this node is refused (docs/suspend-and-resume.md).
    # contentStore = {
    #     url = "s3://legion-content-usw2/",
    #     # Omit auth to use the node's ambient AWS credentials (environment
    #     # variables, then the instance profile). Or read an AWS credential
    #     # object from KV, the same shape artifact auth.kv takes:
    #     # auth = { kv = "legion/content-store" },
    # },
    # Turn a task's declared `resources` into real kernel limits
    # (docs/resource-enforcement.md). On by default: memory pressure,
    # oversubscription, memoryMax, usage reports, and sandbox confinement
    # need it. Set false to treat `resources` as scheduling estimates only.
    # Linux + cgroup v2 with a root worker; a node that cannot enforce logs a
    # warning at startup and runs tasks unconfined.
    resourceEnforcement = {
        enabled = true,
        # Path under /sys/fs/cgroup. The worker creates it and one subtree
        # per allocation beneath it.
        cgroupParent = "legion",
        # Pack this node past its physical capacity: schedulable memory is
        # (physical - reservedResources.memory) x memory. Above 1.0 a task
        # within its reservation can be stopped under memory pressure (the
        # `evict-reserved` step), so use it where workloads are idle-heavy.
        # CPU only changes how much the scheduler places, and `cores` is
        # never oversubscribed. Needs `enabled = true`. Default 1.0 (off).
        # oversubscribe = { memory = 1.0, cpu = 1.0 },
        # What the node does when it runs short of memory
        # (docs/resource-enforcement.md#memory-pressure). On whenever
        # `enabled` is. Every field is optional. Values below are the
        # defaults except freeBelow, which scales with the node.
        # memoryPressure = {
        #     enabled = true,
        #     # MemAvailable below this triggers the response. Default 5% of
        #     # node memory, at least 256MiB and at most 4GiB.
        #     freeBelow = "2GiB",
        #     # ...or memory PSI "some" (10s average) above this.
        #     stallAbove = "10%",
        #     # Wait this long after each action before taking another.
        #     settle = "5s",
        #     # Always run in this order. Remove one to forbid it.
        #     # `reclaim` gives back memory above reservations (a sandbox's
        #     # balloon, `memory.reclaim` for the rest), `suspend` checkpoints
        #     # tasks that set suspend.onPressure = true (most idle first),
        #     # and `evict-reserved` runs only when oversubscribe.memory > 1.
        #     steps = ["reclaim", "suspend", "evict-bursting", "evict-reserved"],
        # },
        # The share of each suspended instance's memory and cpu this node
        # keeps reserved, so resumes pinned here find room
        # (docs/suspend-and-resume.md#capacity-for-resumes). Each suspended
        # instance holds max(its job's suspend.reserve, this). A ratio from 0
        # to 1. 1.0 also keeps exclusive cores. Needs no cgroups, so it works
        # with `enabled = false`. Default 0.0: suspended instances hold
        # nothing unless their job asks.
        # suspendedReserve = 0.0,
    },
    # `sandbox` runtime settings. Only used with resourceEnforcement.enabled
    # (docs/resource-enforcement.md#sandbox-tasks).
    sandbox = {
        # MiB a sandbox allocation's cgroup allows on top of its guest
        # memory, for the VMM process itself. Default 64.
        vmmOverheadMiB = 64,
    },
    # Whether this node serves `Worker.StartClaim` — that is, whether it will
    # start work on a boot ticket at all (docs/edge-boot.md). Off by default
    # and independent of the manager's `edgeBoot.enabled`: an operator who
    # turns the feature on cluster-wide still chooses which nodes are edges.
    # edgeBoot = {
    #     acceptClaims = false,
    #     # How often the cached JWKS is proactively refreshed. A fetch
    #     # failure never invalidates cached keys — a node cut off from its
    #     # managers must still be able to redeem tickets it can verify.
    #     jwksRefresh = "5m",
    #     # Tolerance for clock disagreement with the minting manager.
    #     clockSkew = "30s",
    # },
}
