{
    name: "sidecar-example",
    tasks: [
        {
            name: "api",
            kind: "noop",
            lifecycle: "service",
            group: "app",
        },
        {
            name: "logger",
            kind: "exec",
            lifecycle: "sidecar",
            group: "app",
            config: {
                command: ["echo", "tail", "logs"],
            },
        },
    ],
}
