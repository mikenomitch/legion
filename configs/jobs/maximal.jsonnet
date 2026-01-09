{
    name: "maximal-demo",
    version: 1,
    tasks: [
        {
            name: "web",
            count: 2,
            kind: "noop",
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
            count: 1,
            kind: "noop",
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
    ],
}
