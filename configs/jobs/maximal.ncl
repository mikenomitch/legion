{
    name = "maximal-demo",
    version = 1,
    # How important this job is, relative to everything else on the cluster
    # (docs/preemption-and-eviction.md). One of, most important first:
    #
    #   critical   — never preempted; gate it with an ACL `bands` selector
    #   high       — production work that pages someone when it stops
    #   elevated   — matters more than the default, not on the paging path
    #   normal     — the default; everything that does not say otherwise
    #   low        — may be displaced when the cluster is full
    #   background — first to go; no completion expectation at all
    #
    # The band does two things with one field: it sets the job's default
    # eval-queue priority (90/70/60/50/30/15), and it governs preemption.
    # Preemption is strictly CROSS-band — an allocation is only ever a victim
    # of a job in a STRICTLY higher band, never a peer — so choosing a band
    # below `normal` is the consent to be displaced.
    "priority" = "high",
    rollout = {
        strategy = "default",
        progression = "auto",
        # Stateful groups (frontend's slot task, sandboxes' keyed task) cannot
        # use createFirst — two live allocations would share one instance id +
        # storage handle (see docs/stateful-workloads.md). Default to the safe
        # stop-first replacement globally; exercise createFirst on the
        # stateless `prep` group below.
        replacement = "destroyFirst",
        inPlace = false,
        percentSteps = [10, 25, 50, 100],
        stepStagger = "15s",
        allocationStagger = "2s",
        minHealthyTime = "10s",
        healthyDeadline = "5m",
        ignoreHealthChecks = false,
        capacityBlockTimeout = "10m",
        onCapacityTimeout = "rollback",
        rollbackOnFailure = true,
        onHealthDeadline = "rollback",
        batchKillAfter = "30m",
    },
    rolloutByGroup = {
        frontend = {
            # Ordinal-aware rollout for the group's slot-mode stateful task
            # (see docs/stateful-workloads.md "Ordered rollouts for slot
            # pools"). Only valid on groups with a slot-mode stateful task,
            # which is why it is scoped here rather than globally.
            slots = {
                # "sequential" (K8s OrderedReady analog): replace ONE slot
                # at a time, highest ordinal first (count-1 .. 0), each step
                # gated on the previous slot's replacement passing the
                # rollout health criteria. Defines its own one-slot-per-step
                # progression, so it replaces the global step mode
                # (percentSteps above) for this group and cannot be combined
                # with step modes in the same block. Default: "parallel"
                # (today's behavior).
                order = "sequential",
                # Staged/partitioned updates (K8s partition analog): only
                # slots >= updateFrom are updated; lower slots keep running
                # their current allocations. Resubmit with a lower value to
                # promote the new config to more slots (0 = everything).
                # Must be < the slot task's count. Requires order:
                # "sequential". 0 is the same as omitting the field.
                updateFrom = 0,
            },
            inPlace = true,
        },
        # prep is stateless, so createFirst (replace-before-stop) is safe here
        # and exercises the strategy that stateful groups forbid.
        prep = {
            replacement = "createFirst",
        },
        # A gang's replicas place and replace as one unit, so its group's
        # rollout must resolve to a single step: a canary of half a gang is
        # a broken gang (docs/gang-scheduling.md). This overrides the global
        # percentSteps above; validation rejects any multi-step mode here.
        trainer = {
            percentSteps = [100],
        },
    },
    tasks = [
        {
            name = "web",
            count = 2,
            runtime = "exec",
            group = "frontend",
            lifecycle = "service",
            # Cross-group dependency: service and batch tasks may depend on
            # tasks in any group. web is not planned until the prep group's
            # batch task has succeeded. (A service may only take a
            # complete/success condition on a batch task — services never
            # complete.)
            dependsOn = [
                {
                    task = "batch",
                    condition = "success",
                },
            ],
            # Stable instance identity (see docs/stateful-workloads.md):
            # slot mode gives each replica an ordinal slot 0..count-1 whose
            # instance id and storage handle survive reschedules. The task
            # sees LEGION_INSTANCE_ID / LEGION_INSTANCE_SLOT /
            # LEGION_INSTANCE_MODE (and LEGION_ALLOC_ID) in its env.
            stateful = {
                mode = "slot",  # the default; shown for completeness
                # Node-loss fencing (at-most-one; see docs/stateful-workloads.md
                # "Node loss and fencing"): the worker self-fences this task
                # after losing manager contact for selfTerminateAfter; the
                # manager only replaces an allocation on a lost node after
                # replaceAfter (must be > selfTerminateAfter; "never" = wait
                # for node recovery or `legion instance replace`). Optional:
                # defaults are selfTerminateAfter 90s, replaceAfter 2x that.
                onLost = {
                    selfTerminateAfter = "90s",
                    replaceAfter = "3m",
                },
                # Per-instance persistent storage, materialized by a volume
                # driver on whatever node the instance lands on (see
                # docs/volumes.md). Each slot gets its OWN volume, reattached
                # on every reschedule — replica 2's disk is replica 2's disk.
                storage = {
                    # Where the task sees it. For `container` tasks this is a
                    # real bind mount; `exec` tasks run in the host mount
                    # namespace and read $LEGION_STORAGE_DIR instead.
                    path = "/var/lib/web",
                    # Volume driver. Defaults to "local-dir" (a directory on
                    # the worker's disk, node-local). "nfs" is the built-in
                    # network option, which lets instances reschedule anywhere.
                    driver = "local-dir",
                    # How the task consumes it: "filesystem" (a directory —
                    # the default, and what exec/container want) or "block" (a
                    # raw device or disk image a microVM attaches). Admission
                    # rejects a mode the task runtime or the plugin cannot serve.
                    mode = "filesystem",
                    # Passed to the driver verbatim; it declares which are
                    # required (`legion volume drivers` lists them).
                    params = { size = "10G" },
                    # K8s persistentVolumeClaimRetentionPolicy analogs (both
                    # default true = Retain). retainOnScaleIn ~ whenScaled;
                    # retainOnStop ~ whenDeleted and governs `job stop` only
                    # (`job destroy` always deletes every instance record).
                    retainOnScaleIn = true,
                    retainOnStop = true,
                },
            },
            # Claims on storage that exists independently of any instance —
            # an operator's host path, an NFS export, scratch space (see
            # docs/volumes.md). Unlike stateful.storage these are shared, are
            # never provisioned or reclaimed by instance churn, and are valid
            # on STATELESS tasks too. Each becomes LEGION_VOLUME_<NAME>.
            volumes = [
                {
                    name = "models",
                    # The `host` driver claims a path the operator declared in
                    # the worker config's `hostVolumes` map, by name.
                    driver = "host",
                    params = { name = "models" },
                    path = "/models",
                    readOnly = true,
                },
            ],
            config = {
                command = ["sleep", "1"],
            },
            network = {
                # Datapath: auto | host | gated (docs/network-handlers.md).
                # `gated` (Linux, container and exec) gives the task an empty
                # network namespace holding loopback and the datapath only —
                # upstream ports, VIPs, its resolver (127.0.0.53 when it has
                # no VIP block), and an egress proxy at 127.0.0.1:1080
                # exported as HTTP_PROXY/HTTPS_PROXY/ALL_PROXY (and lower
                # case). Egress nothing allows is denied (`gated-default`),
                # and the host guard refuses the node and cloud metadata
                # whatever policy says. `auto` (the default) is `gated` for a
                # `container` on a Linux node and `host` everywhere else
                # (exec, macOS; sandboxes per docs/task-runtime-sandbox.md).
                # `host` opts a container out. A `require` policy can impose
                # a mode for a pool. `bridge` / `cni/<name>` are reserved for
                # the datapath plugin.
                mode = "auto",
                ports = [
                    # Gated: the datapath owns the host port and forwards to
                    # 8080 inside the task's namespace. Host: dynamic host
                    # port (macOS publishes it to 8080).
                    { name = "http", to = 8080 },
                    { name = "metrics", static = 9090 },  # fixed host port
                ],
                # Attribution keys carried on every net.denied event and
                # access-log line (never on a metric label). Stored policies
                # can select on them too.
                labels = { team = "web", tier = "prod" },
                # Inline egress policy: the same shape as a stored policy
                # object, scoped to this task. Merged after every matching
                # `require` policy; replaces a matching `default` one.
                egress = {
                    protocol = "tcp",         # tcp (default) | http — never inferred
                    policy = "deny",          # fallthrough: allow | deny (deny when `allow` is used)
                    accessLog = false,
                    # Sugar: host, host:port, cidr, cidr:port — and host/path
                    # once protocol = "http". Desugars to allow rules ahead of
                    # `rules`.
                    allow = ["*.internal.example.com", "10.0.0.0/8:5432"],
                    rules = [
                        # First match wins. Connect-time fields (host, port,
                        # cidr, identity) never touch the byte path; path /
                        # method / header need protocol = "http".
                        { when = { host = "telemetry.example.com", port = 443 }, action = "allow" },
                        { when = { cidr = "169.254.0.0/16" }, action = "deny", reason = "no metadata service" },
                    ],
                    # Handlers: code on the path, only when policy is not
                    # enough, run in order after the rules allow the flow.
                    # Each is written as its kind, with the kind's settings
                    # inside it:
                    # authz = a webhook verdict, cached per (identity,
                    # destination) for `cache` (default 10s, max 5m);
                    # proxy = hand the flow to any proxy (PROXY v2 header on
                    # TCP, Legion-* headers on HTTP): a catalog `service`
                    # (`local = true` for its endpoints on this node only) or
                    # a sidecar's `port` label;
                    # dlp = reject HTTP bodies that match (needs HTTP).
                    # Every hop: timeout (default 2s) and onError (what a
                    # failed hop means: deny, the default, or allow).
                    handlers = [
                        { authz = { url = "http://policy.internal/check", cache = "30s" }, timeout = "2s", onError = "deny" },
                        { proxy = { service = "egress-filter", local = true, proxyProtocol = true }, onError = "deny" },
                    ],
                },
                # Inline ingress policy in front of this task's advertised
                # ports: the datapath binds the advertised port, the task
                # binds a private twin (docs/network-handlers.md, "Ingress").
                ingress = {
                    protocol = "http",
                    rules = [
                        { when = { path = "/admin/**" }, action = "deny", reason = "not externally reachable" },
                        # Peer identity is known with tls.mode = "mutual".
                        { when = { identity = "auto:web:*" }, action = "allow" },
                    ],
                    # TLS on the advertised port. server | mutual with
                    # certFrom = "legion" (the node's mesh certificate; peers
                    # verified against the mesh CA with clientCa = "legion"),
                    # certFrom = { kv = "<path>" } (a PEM bundle in KV: the
                    # chain, then the key; swapped live on `kv put`, and the
                    # submitter needs kv read on the path), or
                    # certPath/keyPath (+ clientCa bundle) for your own PKI.
                    tls = {
                        mode = "off",          # off | server | mutual
                        # certFrom = "legion",
                        # certFrom = { kv = "certs/web.example.com" },
                        # clientCa = "legion",
                        # requireIdentity = ["auto:web:*"],
                    },
                },
            },
            service = {
                name = "web",       # defaults to task name
                port = "http",      # label from network.ports; or a number/'dynamic' to declare inline
                tags = ["v1", "public"],
                meta = { protocol = "http" },
                # Connect intentions (see docs/service-discovery.md): job ids
                # allowed to reach this service through a `connect` upstream.
                # Omit the field (or use ["*"]) to allow every job. Denied
                # consumers simply get an empty endpoint list; enforcement is
                # control-plane + forwarder, not cryptographic identity.
                allow = ["gateway", "maximal-demo"],
            },
            # Services this task dials, each via a node-local loopback
            # forwarder that load-balances across the upstream's healthy
            # endpoints. The task reads $LEGION_UPSTREAM_<SERVICE>
            # ("127.0.0.1:<port>") and $LEGION_UPSTREAM_PORT_<SERVICE>.
            # The named service need not exist yet.
            connect = {
                upstreams = [
                    # Minimal form: loopback port assigned at placement out of
                    # the same per-node range as host ports.
                    { service = "db" },
                    # Pin the loopback port for clients that cannot read env
                    # vars. Must not collide with a static port above.
                    { service = "cache", localPort = 6379 },
                    # Strategy override: least_latency (default; two random
                    # picks, the lower latency x load wins) | round_robin |
                    # random | least_conn | local_first (same-node endpoints
                    # first, widening when none is healthy locally).
                    { service = "search", balance = "least_conn" },
                    # HTTP framing and traffic management on a service
                    # upstream (docs/network-handlers.md): a per-request
                    # deadline, bounded retries (idempotent methods only by
                    # default), a canary split by service tag, and outlier
                    # ejection.
                    {
                        service = "api",
                        http = {
                            timeout = "3s",
                            retries = { attempts = 2, on = ["5xx", "connect-failure"], budget = "20%" },
                            split = [{ tag = "canary", weight = 10 }],
                            outlier = { consecutive5xx = 5, ejectFor = "30s" },
                        },
                    },
                    # An external upstream: `name` + `to` instead of a
                    # service. The task gets $LEGION_UPSTREAM_OPENAI on
                    # loopback and speaks plaintext; Legion originates TLS
                    # (https / tls schemes), so the body is visible to
                    # handlers without intercepting anyone's TLS. The scheme
                    # also decides HTTP framing (http / https).
                    {
                        name = "openai",
                        to = "https://api.openai.com",
                        handlers = [
                            {
                                dlp = { maxBytes = "1Mi", patterns = ["(AKIA|sk-)[A-Za-z0-9]{16,}"] },
                                onError = "deny",
                            },
                        ],
                        # Credential injection (docs/network-handlers.md,
                        # "Credential injection"): the node sets these headers
                        # on every request to the origin, replacing whatever
                        # the task sent, with values fetched from KV. The task
                        # never holds the key (give its SDK a placeholder).
                        # https:// external upstreams only; the submitter
                        # needs kv:read on each path.
                        credentials = [
                            { header = "Authorization", prefix = "Bearer ", from = { kv = "secrets/openai" } },
                            { header = "OpenAI-Organization", from = { kv = "secrets/openai-org" } },
                        ],
                    },
                    {
                        name = "billing",
                        to = "https://billing.internal.example",
                        # The identity provider: the manager mints this
                        # task's "billing" workload identity for the
                        # allocation and the node injects it, asking for a
                        # new one once half its TTL has passed.
                        credentials = [
                            { header = "Authorization", prefix = "Bearer ", from = { identity = "billing" } },
                        ],
                    },
                ],
                # Config files rendered from the upstreams and their live
                # endpoint feed (docs/service-discovery.md, "Templates"):
                # written before the task starts and on every heartbeat
                # that changes the result. Functions: service "<key>"
                # [host|port], range endpoints "<key>" (.address .port
                # .node .allocation .tags), env "<NAME>".
                templates = [
                    {
                        name = "backends",
                        destination = "local/backends.cfg",   # under the task directory
                        source = "db = {{ service \"db\" }}\n{{ range endpoints \"search\" }}server {{ .address }}:{{ .port }} # {{ .node }}\n{{ end }}",
                        changeMode = "signal",     # noop (default) | signal | restart
                        changeSignal = "SIGHUP",
                        coalesce = "2s",           # let the feed settle before acting
                        splay = "5s",              # random delay before the action
                    },
                ],
            },
            # Workload identities (see docs/workload-identity.md): each named
            # identity gets its own short-lived OIDC token, delivered as a
            # file and/or env var and refreshed in place by the worker.
            # Requires workloadIdentity.enabled on the manager; inert
            # otherwise.
            identities = [
                {
                    name = "aws",
                    audience = ["sts.amazonaws.com"],
                    ttl = "15m",
                    file = {
                        path = "/var/run/legion/identity/aws/token",
                        mode = "0400",
                    },
                    changeMode = "noop",
                },
                {
                    # Reserved control-plane identity: authenticates back to
                    # Legion itself. Omit `audience` — the manager owns it and
                    # stamps aud = the cluster issuer.
                    name = "legion",
                    file = {
                        path = "/var/run/legion/identity/legion/token",
                        mode = "0400",
                    },
                    changeMode = "noop",
                },
                {
                    # Injection only: no env or file, because the "billing"
                    # upstream's credential names it (from = { identity }).
                    # The node holds the token; the task never sees it.
                    name = "billing",
                    audience = ["https://billing.internal.example"],
                    ttl = "10m",
                },
            ],
            # Task-level KV policy defaults; entries override field-by-field
            # (see docs/kv-store.md "Reactive updates"). Keys are camelCase.
            kv = {
                watch = true,
                onChange = "restart",
                onDelete = "fail",
            },
            env = [
                {
                    name = "FEATURE_FLAGS",
                    value = {
                        template = "{{ kv \"flags/global\" }}",
                    },
                    # watch is inherited from the task defaults above.
                    kv = {
                        onChange = "rollout",
                        onDelete = "none",
                    },
                },
            ],
            files = [
                {
                    path = "/tmp/legion/runtime.conf",
                    mode = "0640",
                    file = {
                        template = "tenant={{ kv \"tenant/id\" }}\n",
                    },
                },
                # onChange = "update" (files only): rewrite the file under the
                # running task instead of replacing the allocation, then apply
                # changeMode: noop (default) | signal (+ changeSignal) |
                # restart (in place). splay spreads a fleet's reloads.
                {
                    path = "/tmp/legion/tls/bundle.pem",
                    mode = "0400",
                    file = {
                        template = "{{ kv \"certs/web.example.com\" }}",
                    },
                    kv = {
                        onChange = "update",
                    },
                    changeMode = "signal",
                    changeSignal = "SIGHUP",
                    splay = "10s",
                },
            ],
            resources = {
                # MHz. `cpus` is an older alias for `cpu`, also in MHz (not
                # a CPU count): `cpus = 1` reserves 1 MHz, and submit warns
                # about any value under 10. Use `cores` for whole CPUs.
                cpu = 1500,
                memory = 512,
                disk = 1024,
            },
            constraints = {
                role = "web",
                region = "us-east",
            },
            permit = [
                {
                    key = "pool",
                    operator = "equal",
                    value = "default",
                },
            ],
            checks = [
                {
                    type = "boot",
                    name = "HTTP Boot /health",
                    kind = "http",
                    path = "/health",
                    port = "http",  # port label -> resolved to the assigned host port
                    every = "5s",
                    timeout = "2s",
                    unhealthy_after_count = 1,
                },
                {
                    type = "health",
                    kind = "http",
                    path = "/health",
                    port = "http",  # port label -> resolved to the assigned host port
                    every = "10s",
                    timeout = "2s",
                    unhealthy_after_count = 1,
                },
            ],
            on_unhealthy = {
                restart_count = 3,
                restart_period = "30m",
                restart_delay = "30s",
                move_count = 3,
                move_period = "30m",
                # The first reschedule's wait. It doubles for each earlier
                # move within move_period, capped at 30m.
                move_delay = "30s",
            },
            # What happens when the task PROCESS exits, as opposed to a health
            # check failing (see docs/task-supervision.md). Both paths spend
            # the same local restart budget above; this block only decides
            # whether an exit is worth restarting for.
            restart = {
                # always | onFailure | never. Default: "always" for
                # service/sidecar, "onFailure" for batch.
                policy = "always",
                # Wait before relaunching. Defaults to
                # on_unhealthy.restart_delay when omitted.
                delay = "5s",
                # Exit codes counted as success. Default [0].
                successExitCodes = [0],
            },
            # How the platform terminates this task, whoever initiates it:
            # `job stop`, a rollout replacement, a drain, a reschedule, a
            # scale-in, or preemption all use this one ladder
            # (docs/graceful-stop.md). Both fields are optional.
            stop = {
                # The notice signal. One of SIGTERM (default), SIGINT,
                # SIGQUIT, SIGHUP, SIGUSR1, SIGUSR2. SIGKILL is deliberately
                # not offered: it is the end of the ladder, not a notice.
                signal = "SIGTERM",
                # Grace between the signal and SIGKILL. Default 30s, capped
                # at 5m — grace may slow a drain, never wedge one.
                killAfter = "30s",
                # Only for tasks with a `service` block: how long the task
                # keeps serving after it has left the catalog and before the
                # signal is sent, so consumers on other nodes learn it is
                # gone first (docs/graceful-stop.md). Unset = one heartbeat
                # interval of the node; "0s" disables; capped at 5m.
                drain = "5s",
            },
            # A floor on how many of this task's allocations VOLUNTARY
            # disruption may take (docs/preemption-and-eviction.md).
            # Preemption is the only consumer today. Deliberately does not
            # bind drain, node loss or `job stop`: a floor that can block
            # maintenance is the documented cluster-wedge.
            disruption = {
                # Absolute count (2) or percent of count ("50%"). Percentages
                # round UP: "50%" of 5 protects 3. Default 0 — choosing a
                # preemptible band donates the whole task.
                minAvailable = "50%",
            },
            # How far a node may get on a start for this task BEFORE the
            # placement is durable (docs/edge-boot.md). Absent = `off`,
            # today's behavior: dispatch happens after the commit and the
            # wire payload carries no provisional block at all.
            speculativeStart = {
                # 'off' (default) | 'stage' | 'boot' | 'serve'.
                #   stage — fetch artifacts, pull the image, build the root
                #           disk; do not start the workload.
                #   boot  — also boot the guest, with its network held shut.
                #   serve — also open the network immediately. The only mode
                #           where a workload that LOSES its race may already
                #           have had externally visible side effects.
                # A task with declared ports, connect upstreams, or storage
                # whose driver cannot fence a concurrent attach is downgraded
                # to `stage` — reported by `legion job validate`.
                mode = "stage",
                # Lease = unratified work is killed at expiry, unconditionally.
                # Default 5s, capped at 2m.
                ratifyTimeout = "5s",
                # What to do when ratification is refused. 'kill' (default,
                # a hard destroy) or 'stop' (the normal graceful-stop ladder).
                onRefused = "kill",
            },
            affinities = [
                {
                    key = "zone",
                    operator = "=",
                    value = "us-east-1a",
                    multiplier = 2.0,
                },
                {
                    key = "instance_type",
                    operator = "matches",
                    value = "^c[0-9].*",
                    multiplier = 1.0,
                },
            ],
        },
        {
            name = "batch",
            count = {
                num = 3,
                per = "region",
            },
            runtime = "exec",
            group = "prep",
            lifecycle = "batch",
            schedule = "every 15m",
            # Artifacts fetched into the task directory before start
            # (docs/artifacts.md). A bare string is the minimal form; the
            # object form adds naming, verification, unpack, pinning, and
            # auth controls.
            artifacts = [
                # Minimal = fetch + auto-unpack into the task directory.
                "https://github.com/acme/tool/releases/download/v1.4/tool-linux-amd64.tar.gz",
                {
                    source = "s3://acme-builds/api/v42.tar.gz",
                    # Handle for ${artifact.build} / LEGION_ARTIFACT_BUILD.
                    name = "build",
                    # Relative to the task directory.
                    dest = "build/",
                    checksum = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
                    # auto (default) | archive | file
                    unpack = "auto",
                    # Credential JSON read from Legion KV at fetch time.
                    auth = { kv = "secrets/build-store" },
                },
                {
                    source = "git::https://github.com/acme/repo?ref=v1.2.0",
                    name = "repo",
                    dest = "src/",
                    # pin (default) resolves the ref to a commit SHA at
                    # submit; always re-resolves at every staging.
                    resolve = "always",
                },
                {
                    source = "oci://registry.acme.io/models/embedder@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    name = "model",
                    # ORAS layer selection by media type.
                    mediaType = "application/vnd.acme.model.weights.v1",
                    dest = "models/",
                },
                {
                    source = "https://releases.acme.io/bin/runner",
                    name = "runner",
                    unpack = "file",
                    # chmod for single-file artifacts.
                    mode = "0755",
                },
            ],
            config = {
                command = ["sleep", "1"],
                env = {
                    # Worker-side interpolation of staged artifact paths.
                    MODEL_DIR = "${artifact.model}",
                    BUILD_DIR = "${artifact.build}",
                },
            },
            resources = {
                cpu = 500,
                memory = 256,
            },
            constraints = [
                {
                    key = "cpu",
                    value = "amd",
                },
                {
                    key = "zone",
                    operator = "distinct",
                },
            ],
            affinities = {
                node_class = "spot",
            },
        },
        {
            name = "shell",
            count = 1,
            runtime = "exec",
            group = "frontend",
            lifecycle = "sidecar",
            # Sidecars are group-bound: they run alongside their group's
            # primary task, so they may only depend on tasks in their own
            # group (a cross-group dependency here is rejected at
            # validation — put it on the group's primary instead, as web
            # does above).
            dependsOn = [
                {
                    task = "web",
                    condition = "start",
                },
            ],
            config = {
                command = ["echo", "hello", "from", "exec"],
                env = {
                    LEGION_TASK = "exec",
                },
                cwd = ".",
                user = "nobody",
                oom_adj = 100,
                cgroup = "legion/demo",
                landlock = {
                    read_only_paths = ["/usr", "/bin"],
                    read_write_paths = ["/tmp"],
                },
                # Process hardening (Linux; ignored with a warning elsewhere).
                # See docs/task-supervision.md.
                hardening = {
                    # prctl(PR_SET_NO_NEW_PRIVS): no privilege gain through
                    # setuid binaries or file capabilities, ever.
                    noNewPrivileges = true,
                    # Private mount namespace + a tmpfs over /tmp, invisible
                    # to every other task on the node. Needs a worker with
                    # CAP_SYS_ADMIN.
                    privateTmp = true,
                    # Bounding-set drops. ["all"] drops everything; named
                    # capabilities work with or without the CAP_ prefix.
                    # Needs CAP_SETPCAP on the worker.
                    capabilities = { drop = ["all"] },
                    # Octal file-mode creation mask.
                    umask = "0027",
                    # Scheduling niceness, -20..=19.
                    nice = 5,
                },
            },
            resources = {
                cpu = 100,
                memory = 128,
            },
            constraints = {
                os = "linux",
            },
            affinities = {
                region = "us-west",
                same_node_as = "web",
            },
        },
        {
            name = "sandbox",
            runtime = "exec",
            group = "sandboxes",
            lifecycle = "service",
            # Keyed-mode stateful task (see docs/stateful-workloads.md):
            # instances are created/destroyed on demand via
            # `legion instance create maximal-demo --group sandboxes` /
            # `legion instance destroy <id>`. count does NOT drive keyed
            # tasks — zero instances means zero allocations — and each id is
            # never recycled after destroy. The task sees
            # LEGION_INSTANCE_ID / LEGION_INSTANCE_MODE ("keyed") and
            # LEGION_ALLOC_ID in its env (no LEGION_INSTANCE_SLOT).
            stateful = {
                mode = "keyed",
                # Reject `instance create` without --id. Default false: the
                # server mints an id when --id is omitted. Explicit ids are
                # always honored.
                requireId = true,
                storage = {
                    # Opaque per-instance handle, reattached across
                    # reschedules and dropped on destroy.
                    path = "/workspace",
                },
            },
            config = {
                command = ["sleep", "600"],
            },
            # The port `suspend.wake` routes gateway requests to.
            network = {
                ports = [{ name = "http" }],
            },
            # How `legion instance suspend` checkpoints this keyed task and
            # how `legion instance start` resumes it
            # (docs/suspend-and-resume.md). Keyed stateful tasks only.
            suspend = {
                # "memory" | "disk". Unset: memory for a sandbox, disk for
                # every other runtime. An `exec` task suspends to disk: the
                # task stops, the volume stays in place, and a resume boots
                # it fresh on that volume.
                checkpoint = "disk",
                # How much of the reservation stays charged to the node while
                # suspended, so the resume finds room (docs/
                # suspend-and-resume.md, "Capacity for resumes"). false
                # (default, the density win), true (everything, exclusive
                # cores included), a ratio of memory and cpu MHz between 0
                # and 1, or fixed amounts { memory = "1GiB", cpu = 100 } no
                # larger than `resources`.
                reserve = 0.25,
                # When a resume cannot restore (checkpoint lost with its
                # node, compat drift, a failed restore): "cold" (default)
                # boots fresh on the retained storage, "fail" leaves the
                # instance suspended with the error in its detail.
                onRestoreFailure = "cold",
                # How long a resume waits to be placed on its checkpoint's
                # node before onRestoreFailure = "cold" boots it fresh.
                resumeTimeout = "30s",
                # Where the checkpoint lives. "local" (default): only on the
                # node that took it, and a resume is pinned there. "store":
                # the node also publishes it to its worker `contentStore`, so
                # the instance resumes on any compatible node and survives
                # losing the node. "store" needs instance data that can
                # move: no stateful.storage, or a network driver such as
                # nfs. This task's local-dir volume keeps it at "local"
                # (job validation refuses "store" here).
                durability = "local",
                # Once a "store" checkpoint is published, move its record
                # out of the replicated store into the bucket, next to its
                # parts (docs/suspend-and-resume.md, "Archiving idle
                # instances"). "slim": the instance row stays, recording the
                # archive manifest's URL, and the snapshot row is dropped.
                # "manifestOnly": an unnamed instance without a
                # stateful.storage volume keeps no row at all (any other
                # instance keeps a slim row). Unset (default): nothing is
                # archived. Needs durability = "store", a memory checkpoint,
                # and no reserve, so this exec task leaves it out (job
                # validation refuses it here):
                # archive = "slim",
                # argv a sandbox guest runs after every memory restore, once
                # its identity is refreshed. Sandbox memory checkpoints only,
                # so this exec task leaves it out (job validation refuses it
                # here):
                # onResume = ["/usr/local/bin/reconnect"],
                # Suspend automatically when quiet. The worker tracks the
                # listed signals and the manager suspends the instance once
                # none has fired for `after` (needs resourceEnforcement on
                # the worker, which is what measures activity).
                idle = {
                    # Quiet this long, then suspend. Default "3m", at least
                    # "30s".
                    after = "3m",
                    # What counts as activity. Default
                    # ["network", "cpu", "exec"]. "connections" also counts
                    # an open ingress connection through the node datapath
                    # that sends nothing (a client waiting on a slow
                    # response). A sandbox that is not gated sends its own
                    # traffic around the datapath, so "connections" is
                    # refused there, as is
                    # "exec" on a sandbox pinned to a VMM (a microVM takes no
                    # exec sessions). On a sandbox, "network" also counts
                    # bytes the guest sends on its TAP or Apple container
                    # NIC. "hint" (the workload's own signal) is not
                    # available yet and is refused.
                    signals = ["network", "cpu", "exec", "connections"],
                    # The cpu signal's threshold, as a fraction of this
                    # task's cpu. Default 0.02.
                    cpuBelow = 0.02,
                },
                # Let the node's memory pressure response suspend this
                # instance (the `suspend` step) before it evicts anything.
                # Default false.
                onPressure = true,
                # Wake on request (docs/gateway.md): `legion start gateway`
                # routes requests for this instance to `port` (a
                # network.ports label), and a request for a suspended
                # instance resumes it and is held until the instance
                # accepts connections. Without it the gateway refuses the
                # instance.
                wake = {
                    port = "http",
                    # How long the gateway holds a request while the
                    # instance resumes, then 503 with Retry-After. Default
                    # "30s", from "1s" to "5m".
                    timeout = "30s",
                },
            },
            resources = {
                cpu = 250,
                memory = 128,
            },
            constraints = {
                os = "linux",
            },
            affinities = {
                node_class = "sandbox",
            },
        },
        {
            name = "scratch",
            runtime = "exec",
            group = "scratch",
            # Session-mode task (see docs/session-mode.md): `legion instance
            # create maximal-demo --group scratch` without --id starts an
            # unnamed session straight on a node with envelope room and
            # writes nothing to the replicated store. It is lost with its
            # node, listed from the nodes' reports, and becomes a keyed
            # instance with `legion instance promote <id>`. The task sees
            # LEGION_INSTANCE_MODE = "session". A session task may not
            # declare storage, volumes, ports, a service, connect upstreams,
            # identities, requireId, or initialInstances.
            stateful = {
                mode = "session",
                # The room each eligible node keeps for this task's sessions.
                # Legion places one generated `scratch-envelope` allocation
                # per node (on workers with the `sessions` capability) with
                # these resources, and admits sessions against it.
                envelope = {
                    # Must set cpu and memory, and every amount `resources`
                    # below declares; a session must fit.
                    resources = { cpu = 4000, memory = 4096 },
                    # Extra placement constraints for the envelopes, on top
                    # of the task's own `constraints`. Optional.
                    constraints = { node_class = "sandbox" },
                    # An elastic envelope (docs/session-mode.md, "Elastic
                    # envelopes"): `resources` above becomes the floor each
                    # node always holds, and each node grows its envelope up
                    # to this ceiling as its sessions fill it (when they fill
                    # more than 90%), through the scheduler, taking only free
                    # room. Must size exactly the keys `resources` sizes,
                    # each at or above it. Optional; without it (or equal to
                    # `resources`) the envelope is fixed.
                    max = { cpu = 16000, memory = 16384 },
                    # How long a node's sessions must use less than half of
                    # its envelope before the node gives the room back.
                    # Default "10m", at least "1m". Only with `max`.
                    shrinkAfter = "10m",
                },
                # Whether a caller-chosen id is accepted (`instance create
                # --id`, a keyed instance from birth; `instance promote
                # --id`, a rename). Default true. With false, every create
                # starts an unnamed session and a promotion keeps its id.
                promote = {
                    onName = true,
                },
                # How a manager's session router picks the node a session
                # starts on (docs/session-mode.md, "Picking a node").
                # "spread" (the default): the node with the most room left.
                # "fast": the roomier of two random nodes with room, the same
                # cost on any fleet size, for a less even spread.
                placement = "spread",
                # How long a node may reach neither a manager nor its
                # contentStore before it suspends this task's running
                # sessions on its own disk, so a copy cut off from the
                # cluster stops before a moved copy can start
                # (docs/session-mode.md, "No split brain"). On reconnect it
                # resumes only the sessions whose manifest still names it.
                # Default "2m", at least "10s". Session tasks with
                # suspend.durability = "store" only.
                fenceAfter = "2m",
            },
            config = {
                command = ["sleep", "600"],
            },
            # Idle sessions (docs/session-mode.md, "Idle sessions"): a
            # session task may suspend. The node captures the checkpoint,
            # frees the session's share of the envelope, and resumes it on
            # `legion instance start <id>` (on another node, with
            # durability = "store", when its own lost it or is full), with
            # no raft entry at any step. Every keyed `suspend` field applies except
            # `reserve` (the envelope is the reservation). This exec task
            # suspends to disk, which keeps nothing but the session's id and
            # spec: a resume boots it fresh under the same id. A `sandbox`
            # session keeps its memory.
            suspend = {
                # "store": every checkpoint is also published, sealed, to the
                # node's contentStore with a session manifest
                # (<contentStore.url>/sessions/<job>/<prefix>/<id>.json), so
                # the session resumes on any node with an envelope, claiming
                # a new generation each time (docs/session-mode.md, "Moving a
                # session"). Needs a contentStore on the workers. "local"
                # keeps the checkpoint on the session's node only, lost with
                # it.
                durability = "store",
                # Suspend a session automatically once quiet (needs
                # resourceEnforcement on the worker). Decided on the node.
                idle = { after = "5m" },
                # How long the node keeps a suspended session's checkpoint
                # before it evicts it, and then how long it remembers the
                # eviction so the session answers `evicted` rather than
                # `not found`. Default "24h", at least "1m". Session tasks
                # only. Disk pressure (content.minFreeDisk) can evict
                # earlier, oldest first.
                keepFor = "24h",
                # What a gracefully stopping worker (SIGTERM, an upgrade)
                # does with running sessions: "keep" (default) leaves them
                # running for the restarted worker to re-adopt;
                # "checkpoint" checkpoints each one first, within the
                # worker's edgeBoot.stopCheckpointTimeout, so a session
                # whose process did not survive is restored on this node.
                # Session tasks only.
                onWorkerStop = "checkpoint",
                # Checkpoint each running session this often without
                # stopping it, replacing the previous checkpoint. Off by
                # default; a full memory image per session per interval, at
                # least "1m". Needs a memory checkpoint (runtime = "sandbox"),
                # so this exec task leaves it out (job validation refuses it
                # here). Session tasks only.
                # checkpointEvery = "10m",
            },
            # One session's size, in the envelope's units.
            resources = {
                cpu = 250,
                memory = 256,
            },
            # The task's constraints and affinities place its envelopes
            # (the generated `scratch-envelope` task inherits both, plus
            # envelope.constraints above).
            constraints = {
                os = "linux",
            },
            affinities = {
                node_class = "sandbox",
            },
        },
        {
            # microVM sandbox (see docs/task-kind-sandbox.md). Same shape as
            # the container kind plus what a VM needs: guest size and a
            # kernel. Persistent state is a *block-mode volume*: a guest
            # kernel attaches a block device, not a host directory, so this
            # is the one task kind that sets `mode = "block"` on its storage.
            name = "microvm",
            runtime = "sandbox",
            group = "microvms",
            stateful = {
                mode = "slot",
                storage = {
                    # Where the guest init mounts the volume. The local-dir
                    # driver serves a formatted disk image; the worker hands
                    # it to the kind; the VMM attaches it as /dev/vdb.
                    path = "/workspace",
                    mode = "block",
                    params = { size = "1G", fs = "ext4" },
                },
            },
            config = {
                image = "alpine:latest",
                command = ["/bin/sh", "-c", "echo hello from a microVM; sleep 600"],
                env = {
                    LEGION_TASK = "sandbox",
                },
                # Guest working directory (defaults to the image's).
                cwd = "/workspace",
                # Guest vCPUs (default 1), independent of resources.cpus.
                # Guest RAM is not set here: it follows `resources` below
                # (memoryMax, else memory). An explicit `memory_mib` still
                # wins this release, with a submit warning when it disagrees.
                vcpus = 1,
                # Writable scratch in the ephemeral root disk, on top of the
                # image contents (default 256). Anything written outside the
                # volume mount lives here and dies with the allocation.
                rootfs_headroom_mib = 256,
                # "auto" (default) = cloud-hypervisor then firecracker on
                # Linux+KVM, Apple `container` on macOS. "dev" runs without a
                # VM (namespaces only on Linux) for machines without KVM.
                hypervisor = "auto",
                # Uncompressed guest kernel; falls back to
                # LEGION_SANDBOX_KERNEL, then /var/lib/legion/kernels/vmlinux.
                # An ${artifact.<name>} reference works here too.
                kernel = "/var/lib/legion/kernels/vmlinux",
                kernel_args = "quiet",
                # How a memory checkpoint's guest RAM comes back on a resume,
                # snapshot restore, fork, or create from a snapshot. "lazy"
                # (default) runs the guest first and loads each page on first
                # touch (cloud-hypervisor needs v52+ and a userfaultfd, else it
                # copies). "eager" reads all of it before the guest runs, for
                # a workload that touches most of its memory right away.
                memory_restore = "lazy",
                # The guest's own network, for a sandbox that is not gated.
                # This task is gated (the task-level `network` below), and
                # submit refuses a gated sandbox with a `config.network`: a
                # gated guest has no NIC. So it is shown commented out.
                # network = {
                #     # "none" (default, loopback only) or "tap".
                #     mode = "tap",
                #     # The host bridge the guest's NIC joins (required with
                #     # mode = "tap"). Legion creates a TAP per instance
                #     # (`lgf` + a hash of the instance id) on it, recreates
                #     # it on resume, and deletes it with the allocation. The
                #     # guest's address is leased from the bridge's pool in
                #     # the worker config (sandbox.bridges.br0), applied at
                #     # boot and re-leased on every restore and fork, and the
                #     # task only places on nodes that declare br0. The MAC is
                #     # derived from the allocation id.
                #     bridge = "br0",
                # },
            },
            # The task-level network block applies to sandboxes too: "gated"
            # boots the guest with no NIC and a vsock to the node datapath,
            # where the egress policy decides (docs/network-handlers.md,
            # "Sandboxes"). A gated task that nothing allows denies every
            # destination (`gated-default`), so list what it needs.
            # Advertised ports work on a gated sandbox: the node datapath
            # owns the port on the node and dials into the guest over vsock,
            # where the guest agent relays to the workload's
            # 127.0.0.1:$LEGION_PORT_HTTP ("Ingress into a sandbox"). A
            # sandbox that is neither gated nor on a TAP has loopback only,
            # and submit refuses ports on it.
            # "auto" is "host" until the vsock path is exercised on a real
            # VMM.
            # A gated sandbox reaches its upstreams as a container does:
            # the guest agent listens on $LEGION_UPSTREAM_API and on the
            # VIP api.svc.legion resolves to, and relays to the node's
            # upstream listener over vsock ("Sandboxes"). Submit refuses a
            # localPort of 1080 or 53 here (the agent's proxy and resolver).
            # A sandbox that is not gated cannot reach its upstreams.
            connect = {
                upstreams = [{ service = "api" }],
            },
            network = {
                mode = "gated",
                ports = [{ name = "http" }],
                egress = { allow = ["pypi.org", "files.pythonhosted.org"] },
            },
            # Opt into snapshots (`legion snapshot`, docs/suspend-and-resume.md).
            # `ttl` deletes a snapshot this long after it was taken (unset:
            # never), `keep` caps snapshots per instance (default 5, the
            # oldest goes first). `memory` (default true) launches a sandbox
            # on a real VMM ready for memory snapshots. A gated sandbox takes
            # them too: every restore moves its vsock onto the restoring
            # allocation's own datapath socket (Firecracker 1.16.1 or later),
            # and a memory snapshot closes the guest's open connections.
            # `memory = false` takes disk snapshots only.
            snapshots = {
                ttl = "7d",
                keep = 5,
                memory = true,
            },
            resources = {
                cpu = 1000,
                # Reserved: what the scheduler packs on, and the cgroup's
                # memory.low.
                memory = 512,
                # Burst ceiling (MiB, at least `memory`): the guest boots
                # with this much RAM, and with resource enforcement the
                # cgroup's memory.max is this plus the worker's
                # sandbox.vmmOverheadMiB. Above `memory` the task is
                # "bursting", which is what a node's memory pressure
                # response gives back first (docs/resource-enforcement.md).
                memoryMax = 1024,
            },
            # A sandbox takes file-delivered identities under
            # /legion/identity (a relative path resolves there). The worker
            # copies each refreshed token onto the guest's identity disk and
            # the guest rewrites the file, with no restart (changeMode
            # defaults to "noop"). On a memory-checkpointable sandbox, env
            # delivery with "noop" works too: the current token is kept at
            # /legion/identity/<ENV> (docs/workload-identity.md, "Sandboxes").
            identities = [
                {
                    name = "pypi",
                    audience = ["pypi.org"],
                    ttl = "15m",
                    file = { path = "/legion/identity/pypi", mode = "0400" },
                },
            ],
            constraints = {
                # A microVM needs KVM, so keep these on a Linux pool. On macOS
                # the kind falls back to Apple `container` dev mode.
                os = "linux",
            },
            affinities = {
                node_class = "sandbox",
            },
        },
        {
            name = "container",
            count = 1,
            runtime = "container",
            config = {
                image = "alpine:latest",
                command = ["echo", "hello", "from", "container"],
                env = {
                    LEGION_TASK = "container",
                },
                # The Linux privilege floor (docs/task-runtime-container.md,
                # "Security"), shown with its defaults. Either opt-out makes
                # the task host-level (it needs the `host` grant on job:write).
                # Linux only (Apple `container` runs each container in its
                # own VM with its own defaults).
                security = {
                    # Container ids 0-65535 map to a 65,536-id host range of
                    # this allocation's own. false runs container root as
                    # host root.
                    userNamespace = true,
                    # "default" refuses mounts, namespaces, kernel modules,
                    # bpf, the keyring, ... "unconfined" removes the filter.
                    seccomp = "default",
                    # Adjust the default capability set (Docker's defaults
                    # without NET_RAW). Case-insensitive, CAP_ prefix
                    # optional; ALL means every capability. `drop = ["ALL"]`
                    # plus a short `add` list is the least-privilege form.
                    # Drops always work; an add needs the node's worker type
                    # to allow it (container.allowCaps in
                    # configs/workers/*.ncl), or the start fails there.
                    capabilities = {
                        add = ["NET_RAW"],
                        drop = ["MKNOD"],
                    },
                },
                # setrlimit for the container's process. A number sets soft
                # and hard alike; also "unlimited" or { soft, hard }. The
                # only default is nofile = 1048576 (or the worker's own hard
                # limit, if lower).
                ulimits = {
                    nofile = 65536,
                    nproc = { soft = 4096, hard = 8192 },
                },
            },
            resources = {
                # Real limits on Linux when the worker runs with
                # `resourceEnforcement.enabled` (see docs/resource-enforcement.md):
                # memory (MiB) becomes a hard cgroup limit, `cores` becomes
                # *exclusive pinned CPUs* (whole numbers only), and `cpu`
                # (MHz) becomes a proportional cpu.weight. A task declares
                # `cores` OR `cpu`, never both. On macOS the same two fields
                # size the Apple `container` guest VM instead.
                cores = 1,
                memory = 256,
            },
            constraints = {
                os = "macos",
            },
            affinities = {
                region = "us-west",
            },
        },
        {
            name = "trainer",
            runtime = "exec",
            # A gang has its group to itself: a multi-task group already
            # places all of its tasks together on one node, so validation
            # rejects `gang` on a task that shares one.
            group = "trainer",
            count = 4,
            # All-or-nothing placement for this task's replicas
            # (docs/gang-scheduling.md). `gang = true` means every replica
            # places in one plan or none does: a gang that does not fit
            # places NOTHING (nor do the job's other tasks in that pass),
            # holds no capacity while it waits, and its eval blocks until a
            # node that could help has room. A running gang that loses
            # members tops them all up together, and preemption never takes
            # it below its quorum.
            #
            # The long form lowers the quorum for elastic workloads: place
            # once `min` replicas fit together, and add the rest as room
            # appears. `min` must be 1..=count; `min = count` is `true`.
            # Not combinable with count.per or keyed stateful tasks, and the
            # group's rollout must replace it in one step (see
            # rolloutByGroup.trainer above).
            gang = {
                min = 3,
                # How long the gang may wait for its quorum before the job
                # fails (stopping any members already running). Counted from
                # the first time the gang came up short, not per retry.
                # Unset: wait indefinitely. At least 1s.
                placementTimeout = "30m",
                # What a lost member (failed, node lost, drained, evicted)
                # does to the rest when it leaves fewer than `min` running:
                #   "restart" (default) stops the survivors and places the
                #             whole gang again, together, once their stop
                #             grace has passed;
                #   "replace" keeps the survivors and places the missing
                #             members together once they fit;
                #   "fail"    fails the job and stops every member.
                # A loss that leaves the quorum running is always replaced
                # alone. Restarts after a failure spend on_unhealthy's move
                # budget; once it is spent the job fails.
                onMemberLoss = "restart",
            },
            # Every member starts with LEGION_GANG_RANK (0..count-1),
            # LEGION_GANG_SIZE, LEGION_GANG_MIN, and LEGION_GANG_LEADER_ADDR
            # (rank 0's node), fixed when it is placed: what a rendezvous
            # such as torchrun needs. Members start as their workers get
            # them; the rendezvous is where they meet. A gang's
            # speculativeStart is capped at `stage`.
            config = {
                command = ["sleep", "3600"],
            },
            resources = {
                cpu = 500,
                memory = 512,
            },
            constraints = {
                os = "linux",
            },
            affinities = {
                region = "us-west",
            },
        },
    ],
}
