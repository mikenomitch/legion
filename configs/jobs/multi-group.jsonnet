{
    name: "multi-group",
    tasks: [
        {
            name: "frontend",
            kind: "noop",
            lifecycle: "service",
            group: "web",
        },
        {
            name: "worker",
            kind: "noop",
            lifecycle: "service",
            group: "worker",
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
