{
    name: "multi-group",
    tasks: [
        {
            name: "frontend",
            runtime: "exec",
            lifecycle: "service",
            group: "web",
            config: {
                command: ["echo", "frontend"],
            },
        },
        {
            name: "worker",
            runtime: "exec",
            lifecycle: "service",
            group: "worker",
            config: {
                command: ["echo", "worker"],
            },
        },
        {
            name: "metrics",
            runtime: "exec",
            lifecycle: "sidecar",
            group: "worker",
            config: {
                command: ["echo", "metrics"],
            },
        },
    ],
}
