{
    name: "dag-separate-groups",
    tasks: [
        {
            name: "seed",
            runtime: "exec",
            lifecycle: "batch",
            group: "prep",
            config: {
                command: ["echo", "seed"],
            },
        },
        {
            name: "web",
            runtime: "exec",
            lifecycle: "service",
            group: "app",
            dependsOn: [
                {
                    task: "seed",
                    condition: "success",
                },
            ],
            config: {
                command: ["echo", "web"],
            },
        },
    ],
}
