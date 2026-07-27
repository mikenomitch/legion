{
    name: "maximal-demo",
    version: 1,
    rollout: {
        strategy: "default",
        progression: "auto",
        // Stateful groups (frontend's slot task, sandboxes' keyed task) cannot
        // use createFirst — two live allocations would share one instance id +
        // storage handle (see docs/stateful-workloads.md). Default to the safe
        // stop-first replacement globally; exercise createFirst on the
        // stateless `prep` group below.
        replacement: "destroyFirst",
        inPlace: false,
        percentSteps: [10, 25, 50, 100],
        stepStagger: "15s",
        allocationStagger: "2s",
        minHealthyTime: "10s",
        healthyDeadline: "5m",
        ignoreHealthChecks: false,
        capacityBlockTimeout: "10m",
        onCapacityTimeout: "rollback",
        rollbackOnFailure: true,
        onHealthDeadline: "rollback",
        batchKillAfter: "30m",
    },
    rolloutByGroup: {
        frontend: {
            // Ordinal-aware rollout for the group's slot-mode stateful task
            // (see docs/stateful-workloads.md "Ordered rollouts for slot
            // pools"). Only valid on groups with a slot-mode stateful task,
            // which is why it is scoped here rather than globally.
            slots: {
                // "sequential" (K8s OrderedReady analog): replace ONE slot
                // at a time, highest ordinal first (count-1 .. 0), each step
                // gated on the previous slot's replacement passing the
                // rollout health criteria. Defines its own one-slot-per-step
                // progression, so it replaces the global step mode
                // (percentSteps above) for this group and cannot be combined
                // with step modes in the same block. Default: "parallel"
                // (today's behavior).
                order: "sequential",
                // Staged/partitioned updates (K8s partition analog): only
                // slots >= updateFrom are updated; lower slots keep running
                // their current allocations. Resubmit with a lower value to
                // promote the new config to more slots (0 = everything).
                // Must be < the slot task's count. Requires order:
                // "sequential". 0 is the same as omitting the field.
                updateFrom: 0,
            },
            inPlace: true,
        },
        // prep is stateless, so createFirst (replace-before-stop) is safe here
        // and exercises the strategy that stateful groups forbid.
        prep: {
            replacement: "createFirst",
        },
    },
    tasks: [
        {
            name: "web",
            count: 2,
            kind: "exec",
            group: "frontend",
            lifecycle: "service",
            // Stable instance identity (see docs/stateful-workloads.md):
            // slot mode gives each replica an ordinal slot 0..count-1 whose
            // instance id and storage handle survive reschedules. The task
            // sees LEGION_INSTANCE_ID / LEGION_INSTANCE_SLOT /
            // LEGION_INSTANCE_MODE (and LEGION_ALLOC_ID) in its env.
            stateful: {
                mode: "slot",  // the default; shown for completeness
                // Node-loss fencing (at-most-one; see docs/stateful-workloads.md
                // "Node loss and fencing"): the worker self-fences this task
                // after losing manager contact for selfTerminateAfter; the
                // manager only replaces an allocation on a lost node after
                // replaceAfter (must be > selfTerminateAfter; "never" = wait
                // for node recovery or `legion instance replace`). Optional:
                // defaults are selfTerminateAfter 90s, replaceAfter 2x that.
                onLost: {
                    selfTerminateAfter: "90s",
                    replaceAfter: "3m",
                },
                // Per-instance persistent storage, materialized by a volume
                // driver on whatever node the instance lands on (see
                // docs/volumes.md). Each slot gets its OWN volume, reattached
                // on every reschedule — replica 2's disk is replica 2's disk.
                storage: {
                    // Where the task sees it. For `container` tasks this is a
                    // real bind mount; `exec` tasks run in the host mount
                    // namespace and read $LEGION_STORAGE_DIR instead.
                    path: "/var/lib/web",
                    // Volume driver. Defaults to "local-dir" (a directory on
                    // the worker's disk, node-local). "nfs" is the built-in
                    // network option, which lets instances reschedule anywhere.
                    driver: "local-dir",
                    // How the task consumes it: "filesystem" (a directory —
                    // the default, and what exec/container want) or "block" (a
                    // raw device or disk image a microVM attaches). Admission
                    // rejects a mode the task kind or the plugin cannot serve.
                    mode: "filesystem",
                    // Passed to the driver verbatim; it declares which are
                    // required (`legion volume drivers` lists them).
                    params: { size: "10G" },
                    // K8s persistentVolumeClaimRetentionPolicy analogs (both
                    // default true = Retain). retainOnScaleIn ~ whenScaled;
                    // retainOnStop ~ whenDeleted and governs `job stop` only
                    // (`job destroy` always deletes every instance record).
                    retainOnScaleIn: true,
                    retainOnStop: true,
                },
            },
            // Claims on storage that exists independently of any instance —
            // an operator's host path, an NFS export, scratch space (see
            // docs/volumes.md). Unlike stateful.storage these are shared, are
            // never provisioned or reclaimed by instance churn, and are valid
            // on STATELESS tasks too. Each becomes LEGION_VOLUME_<NAME>.
            volumes: [
                {
                    name: "models",
                    // The `host` driver claims a path the operator declared in
                    // the worker config's `hostVolumes` map, by name.
                    driver: "host",
                    params: { name: "models" },
                    path: "/models",
                    readOnly: true,
                },
            ],
            config: {
                command: ["sleep", "1"],
            },
            network: {
                mode: "auto",  // auto | host (datapath plugins add more)
                ports: [
                    { name: "http", to: 8080 },      // dynamic host port -> task 8080
                    { name: "metrics", static: 9090 },  // fixed host port
                ],
            },
            service: {
                name: "web",       // defaults to task name
                port: "http",      // label from network.ports; or a number/'dynamic' to declare inline
                tags: ["v1", "public"],
                meta: { protocol: "http" },
            },
            // Workload identities (see docs/workload-identity.md): each named
            // identity gets its own short-lived OIDC token, delivered as a
            // file and/or env var and refreshed in place by the worker.
            // Requires workloadIdentity.enabled on the manager; inert
            // otherwise.
            identities: [
                {
                    name: "aws",
                    audience: ["sts.amazonaws.com"],
                    ttl: "15m",
                    file: {
                        path: "/var/run/legion/identity/aws/token",
                        mode: "0400",
                    },
                    changeMode: "noop",
                },
                {
                    // Reserved control-plane identity: authenticates back to
                    // Legion itself. Omit `audience` — the manager owns it and
                    // stamps aud = the cluster issuer.
                    name: "legion",
                    file: {
                        path: "/var/run/legion/identity/legion/token",
                        mode: "0400",
                    },
                    changeMode: "noop",
                },
            ],
            // Task-level KV policy defaults; entries override field-by-field
            // (see docs/kv-store.md "Reactive updates"). Keys are camelCase.
            kv: {
                watch: true,
                onChange: "restart",
                onDelete: "fail",
            },
            env: [
                {
                    name: "FEATURE_FLAGS",
                    value: {
                        template: '{{ kv "flags/global" }}',
                    },
                    // watch is inherited from the task defaults above.
                    kv: {
                        onChange: "rollout",
                        onDelete: "none",
                    },
                },
            ],
            files: [
                {
                    path: "/tmp/legion/runtime.conf",
                    mode: "0640",
                    file: {
                        template: 'tenant={{ kv "tenant/id" }}\n',
                    },
                },
            ],
            resources: {
                cpus: 1.5,
                memory: 512,
                disk: 1024,
            },
            constraints: {
                role: "web",
                region: "us-east",
            },
            permit: [
                {
                    key: "pool",
                    operator: "equal",
                    value: "default",
                },
            ],
            checks: [
                {
                    type: "boot",
                    name: "HTTP Boot /health",
                    kind: "http",
                    path: "/health",
                    port: "http",  // port label -> resolved to the assigned host port
                    every: "5s",
                    timeout: "2s",
                    unhealthy_after_count: 1,
                },
                {
                    type: "health",
                    kind: "http",
                    path: "/health",
                    port: "http",  // port label -> resolved to the assigned host port
                    every: "10s",
                    timeout: "2s",
                    unhealthy_after_count: 1,
                },
            ],
            on_unhealthy: {
                restart_count: 3,
                restart_period: "30m",
                restart_delay: "30s",
                move_count: 3,
                move_period: "30m",
                move_delay: "30s",
            },
            affinities: [
                {
                    key: "zone",
                    operator: "=",
                    value: "us-east-1a",
                    multiplier: 2.0,
                },
                {
                    key: "instance_type",
                    operator: "matches",
                    value: "^c[0-9].*",
                    multiplier: 1.0,
                },
            ],
        },
        {
            name: "batch",
            count: {
                num: 3,
                per: "region",
            },
            kind: "exec",
            group: "prep",
            lifecycle: "batch",
            schedule: "every 15m",
            // Artifacts fetched into the task directory before start
            // (docs/artifacts.md). A bare string is the minimal form; the
            // object form adds naming, verification, unpack, pinning, and
            // auth controls.
            artifacts: [
                // Minimal: fetch + auto-unpack into the task directory.
                "https://github.com/acme/tool/releases/download/v1.4/tool-linux-amd64.tar.gz",
                {
                    source: "s3://acme-builds/api/v42.tar.gz",
                    // Handle for ${artifact.build} / LEGION_ARTIFACT_BUILD.
                    name: "build",
                    // Relative to the task directory.
                    dest: "build/",
                    checksum: "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
                    // auto (default) | archive | file
                    unpack: "auto",
                    // Credential JSON read from Legion KV at fetch time.
                    auth: { kv: "secrets/build-store" },
                },
                {
                    source: "git::https://github.com/acme/repo?ref=v1.2.0",
                    name: "repo",
                    dest: "src/",
                    // pin (default) resolves the ref to a commit SHA at
                    // submit; always re-resolves at every staging.
                    resolve: "always",
                },
                {
                    source: "oci://registry.acme.io/models/embedder@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    name: "model",
                    // ORAS layer selection by media type.
                    mediaType: "application/vnd.acme.model.weights.v1",
                    dest: "models/",
                },
                {
                    source: "https://releases.acme.io/bin/runner",
                    name: "runner",
                    unpack: "file",
                    // chmod for single-file artifacts.
                    mode: "0755",
                },
            ],
            config: {
                command: ["sleep", "1"],
                env: {
                    // Worker-side interpolation of staged artifact paths.
                    MODEL_DIR: "${artifact.model}",
                    BUILD_DIR: "${artifact.build}",
                },
            },
            resources: {
                cpus: 0.5,
                memory: 256,
            },
            constraints: [
                {
                    key: "cpu",
                    value: "amd",
                },
                {
                    key: "zone",
                    operator: "distinct",
                },
            ],
            affinities: {
                node_class: "spot",
            },
        },
        {
            name: "shell",
            count: 1,
            kind: "exec",
            group: "frontend",
            lifecycle: "sidecar",
            dependsOn: [
                {
                    task: "batch",
                    condition: "success",
                },
            ],
            config: {
                command: ["echo", "hello", "from", "exec"],
                env: {
                    LEGION_TASK: "exec",
                },
                cwd: ".",
                user: "nobody",
                oom_adj: 100,
                cgroup: "legion/demo",
                landlock: {
                    read_only_paths: ["/usr", "/bin"],
                    read_write_paths: ["/tmp"],
                },
            },
            resources: {
                cpus: 0.1,
                memory: 128,
            },
            constraints: {
                os: "linux",
            },
            affinities: {
                region: "us-west",
                same_node_as: "web",
            },
        },
        {
            name: "sandbox",
            kind: "exec",
            group: "sandboxes",
            lifecycle: "service",
            // Keyed-mode stateful task (see docs/stateful-workloads.md):
            // instances are created/destroyed on demand via
            // `legion instance create maximal-demo --group sandboxes` /
            // `legion instance destroy <id>`. count does NOT drive keyed
            // tasks — zero instances means zero allocations — and each id is
            // never recycled after destroy. The task sees
            // LEGION_INSTANCE_ID / LEGION_INSTANCE_MODE ("keyed") and
            // LEGION_ALLOC_ID in its env (no LEGION_INSTANCE_SLOT).
            stateful: {
                mode: "keyed",
                // "uuid": server mints ids when --id is omitted;
                // "caller": --id is required. Explicit ids always honored.
                idSource: "uuid",
                storage: {
                    // Opaque per-instance handle, reattached across
                    // reschedules and dropped on destroy.
                    path: "/workspace",
                },
            },
            config: {
                command: ["sleep", "600"],
            },
            resources: {
                cpus: 0.25,
                memory: 128,
            },
            constraints: {
                os: "linux",
            },
            affinities: {
                node_class: "sandbox",
            },
        },
        {
            name: "container",
            count: 1,
            kind: "container",
            config: {
                image: "alpine:latest",
                command: ["echo", "hello", "from", "container"],
                env: {
                    LEGION_TASK: "container",
                },
            },
            resources: {
                cpus: 0.2,
                memory: 128,
            },
            constraints: {
                os: "macos",
            },
            affinities: {
                region: "us-west",
            },
        },
    ],
}
