{
    name: "dag-same-group",
    tasks: [
        {
            name: "build",
            kind: "exec",
            lifecycle: "batch",
            group: "app",
            config: {
                command: ["echo", "build"],
            },
        },
        {
            name: "api",
            kind: "noop",
            lifecycle: "service",
            group: "app",
            dependsOn: [
                {
                    task: "build",
                    condition: "success",
                },
            ],
        },
    ],
}
