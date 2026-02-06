{
    name: "dag-separate-groups",
    tasks: [
        {
            name: "seed",
            kind: "exec",
            lifecycle: "batch",
            group: "prep",
            config: {
                command: ["echo", "seed"],
            },
        },
        {
            name: "web",
            kind: "noop",
            lifecycle: "service",
            group: "app",
            dependsOn: [
                {
                    task: "seed",
                    condition: "success",
                },
            ],
        },
    ],
}
