{
    name: "multi-group",
    tasks: [
        {
            name: "frontend",
            kind: "exec",
            lifecycle: "service",
            group: "web",
            config: {
                command: ["echo", "frontend"],
            },
        },
        {
            name: "worker",
            kind: "exec",
            lifecycle: "service",
            group: "worker",
            config: {
                command: ["echo", "worker"],
            },
        },
        {
            name: "metrics",
            kind: "exec",
            lifecycle: "sidecar",
            group: "worker",
            config: {
                command: ["echo", "metrics"],
            },
        },
    ],
}
