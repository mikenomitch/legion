{
    name: "maximal-demo",
    version: 1,
    tasks: [
        {
            name: "web",
            count: 2,
            kind: "noop",
            group: "frontend",
            lifecycle: "service",
            config: {
                message: "Hello from the web task",
                retries: 3,
                labels: ["edge", "primary"],
            },
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
            kind: "noop",
            group: "prep",
            lifecycle: "batch",
            config: {
                script: "echo work",
                env: {
                    RUST_LOG: "info",
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
                oom_score_adj: 100,
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
