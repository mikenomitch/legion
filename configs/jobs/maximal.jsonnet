{
    name: "maximal-demo",
    version: 1,
    rollout: {
        strategy: "default",
        progression: "auto",
        replacement: "createFirst",
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
            countEachStep: 1,
            inPlace: true,
        },
    },
    tasks: [
        {
            name: "web",
            count: 2,
            kind: "exec",
            group: "frontend",
            lifecycle: "service",
            config: {
                command: ["sleep", "1"],
            },
            kv: {
                watch: true,
                on_change: "restart",
                on_delete: "fail",
            },
            env: [
                {
                    name: "FEATURE_FLAGS",
                    value: {
                        template: '{{ kv "flags/global" }}',
                    },
                    kv: {
                        on_change: "rollout",
                        on_delete: "none",
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
