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
                mode: "slot",
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
                storage: {
                    path: "/var/lib/web",
                    // K8s persistentVolumeClaimRetentionPolicy analogs (both
                    // default true = Retain). retainOnScaleIn ~ whenScaled;
                    // retainOnStop ~ whenDeleted and governs `job stop` only
                    // (`job destroy` always deletes every instance record).
                    retainOnScaleIn: true,
                    retainOnStop: true,
                },
            },
            config: {
                command: ["sleep", "1"],
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
                    port: 8080,
                    every: "5s",
                    timeout: "2s",
                    unhealthy_after_count: 1,
                },
                {
                    type: "health",
                    kind: "http",
                    path: "/health",
                    port: 8080,
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
            config: {
                command: ["sleep", "1"],
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
