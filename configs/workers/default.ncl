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
    # cached artifacts. The values below are the defaults, except the
    # budgets, which are left unset so the clamped default shares apply.
    content = {
        # false stages every artifact straight from origin.
        enabled = true,
        # Each budget is a fixed size ("50GiB") or a share of the filesystem
        # holding <worker state dir>/content/ ("20%"), sized once at worker
        # start. An unset budget takes a default share clamped to a range.
        # A share you set is not clamped, so writing "20%" here is not the
        # same as leaving it unset. Shares, defaults included, must add up
        # to at most 100%.
        budgets = {
            # Unset: 10% of the disk, clamped to 1GiB..20GiB.
            # artifacts = "20GiB",
            # Unset: 20% of the disk, clamped to 2GiB..200GiB. Holds memory
            # checkpoints: local ones pinned until resumed or deleted,
            # published ones (suspend.durability = "store") as evictable
            # cache entries.
            # snapshots = "40%",
        },
        # Unpinned content unused this long is removed. "0s" disables expiry.
        ttl = "72h",
        # Evict unpinned content, then refuse new content, before the disk
        # drops below this. The scheduler counts it too: a node whose free
        # disk (plus evictable content) less this floor cannot hold one more
        # memory checkpoint takes no new memory-suspend instances.
        minFreeDisk = "5GiB",
    },
    # Where this worker publishes content (docs/artifacts.md, "Publishing
    # content"). Absent by default: the node publishes nothing. Every part
    # is cut into chunks, and each distinct non-zero chunk is sealed
    # (AES-256-GCM) and lands once at <url>/sealed/<name>, beside a sealed
    # index per part, so the bucket never holds plaintext and a repeat
    # checkpoint uploads only what changed (docs/suspend-and-resume.md, "How
    # published parts are stored"). Any worker that can read the URL and is
    # sent the part's key fetches it back, opened and verified by digest.
    # Expire sealed/ with a bucket lifecycle rule older than your
    # longest-lived checkpoint plus dedupWindow. Point each
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
    #     # The size parts are cut into: 64KiB to 16MiB, a multiple of 4KiB.
    #     # Smaller uploads fewer unchanged bytes per change, and makes more
    #     # objects and requests.
    #     # chunkSize = "1MiB",
    #     # How long chunks are shared between publishes. A chunk is reused
    #     # only in the window it was uploaded in. At least 1m.
    #     # dedupWindow = "24h",
    # },
    # Sessions (docs/session-mode.md, "No split brain"). Only acts on
    # sessions whose task sets suspend.durability = "store", which need a
    # contentStore: their manifests live at
    # <contentStore.url>/sessions/<job>/<id-prefix>/<id>.json.
    sessions = {
        # How often this node lists <contentStore.url>/sessions/fence/<node>/
        # for markers a move left (a session that moved away from this node
        # while it could not be told) and stops those copies. One LIST per
        # interval. At least 1s.
        fenceListEvery = "30s",
    },
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
        # defaults except minFree, which scales with the node.
        # memoryPressure = {
        #     enabled = true,
        #     # MemAvailable below this triggers the response. Default 5% of
        #     # node memory, at least 256MiB and at most 4GiB.
        #     minFree = "2GiB",
        #     # ...or memory PSI "some" (10s average) above this.
        #     maxStall = "10%",
        #     # Wait this long after each action before taking another.
        #     settle = "5s",
        #     # Always run in this order. Remove one to forbid it.
        #     # `reclaim` gives back memory above reservations (a sandbox's
        #     # balloon, `memory.reclaim` for the rest), `suspend` checkpoints
        #     # tasks that set suspend.onPressure = true (most idle first),
        #     # and `evict-reserved` runs only when oversubscribe.memory > 1.
        #     steps = ["reclaim", "suspend", "evict-bursting", "evict-reserved"],
        # },
    },
    # `sandbox` runtime settings.
    sandbox = {
        # MiB a sandbox allocation's cgroup allows on top of its guest
        # memory, for the VMM process itself. Default 64. Only used with
        # resourceEnforcement.enabled (docs/resource-enforcement.md#sandbox-tasks).
        vmmOverheadMiB = 64,
        # The host bridges TAP-wired sandboxes join on this node, each with
        # the pool Legion leases guest addresses from
        # (docs/task-runtime-sandbox.md, "Addressing"). A sandbox with
        # config.network = { mode = "tap", bridge = "br0" } places only on
        # nodes that declare br0 here. The bridge itself, and NAT behind it,
        # are the operator's.
        # bridges = {
        #     br0 = {
        #         # The bridge's network: the guest's netmask.
        #         subnet = "10.88.0.0/16",
        #         # The guest's default route. Never leased.
        #         gateway = "10.88.0.1",
        #         # What Legion leases (default: the whole subnet). Keep it
        #         # out of any DHCP pool on the bridge, and disjoint from
        #         # other nodes' ranges when one L2 segment spans nodes.
        #         range = "10.88.128.0/17",
        #         # The guest's resolv.conf (default: the gateway).
        #         dns = ["10.88.0.1"],
        #     },
        # },
    },
    # `container` runtime settings (docs/task-runtime-container.md,
    # "Capabilities").
    container = {
        # The Linux capabilities a container task on this worker type may
        # end up with after its security.capabilities adjustments (Nomad's
        # allow_caps). A task whose effective set (the default, less drop,
        # plus add) holds anything else fails at start here, naming this
        # field. Case-insensitive, CAP_ prefix optional; ["ALL"] allows
        # everything. Unset: the default set below, so jobs can drop
        # capabilities but add none. Adding SYS_ADMIN, NET_ADMIN, SYS_PTRACE,
        # SYS_MODULE, or ALL lets container root act on the host.
        allowCaps = [
            "AUDIT_WRITE", "CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "KILL",
            "MKNOD", "NET_BIND_SERVICE", "SETFCAP", "SETGID", "SETPCAP",
            "SETUID", "SYS_CHROOT",
            # Not in the default set. Uncomment to let jobs opt into raw
            # sockets (ping, traceroute) with
            # security.capabilities.add = ["NET_RAW"]. A container on the
            # host network can then also read the node's traffic.
            # "NET_RAW",
        ],
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
    #     # Provisional starts (sessions and claims) run through the runtime
    #     # at once. 0 means twice the node's cores.
    #     launchConcurrency = 0,
    #     # How long a start waits for a launch slot before the node refuses
    #     # it with RESOURCE_EXHAUSTED (retryable), so the picker tries the
    #     # next node.
    #     queueTimeout = "1s",
    #     # How long a gracefully stopping worker spends checkpointing
    #     # sessions whose task sets suspend.onWorkerStop = "checkpoint".
    #     stopCheckpointTimeout = "60s",
    # },
    # The host guard (docs/network-handlers.md, "The host guard"): the fixed
    # floor of destinations no allocation on this node may reach through the
    # datapath — loopback, the node's own addresses, link-local (cloud
    # metadata), and the metadata endpoints outside link-local. Job policy
    # can't widen it; only these entries can. Both lists default to empty.
    # hostGuard = {
    #     # Holes, one service each: `ip:port` or `cidr:port`. Never a bare
    #     # address or range.
    #     allow = ["10.0.1.7:9100"],
    #     # Extra destinations to refuse like metadata: IPs or CIDRs.
    #     deny = ["10.0.0.2/32"],
    # },
    # Direct exec (docs/exec-and-actions.md, "Direct exec"): admit exec
    # tickets on this node's worker plane, so `legion exec --direct` and
    # platform clients run sessions here with no manager on the data path.
    # Off by default: with it on, any client that can reach port 4848 and
    # holds a ticket for this node can open the session the ticket names.
    # exec = { direct = true },
}
