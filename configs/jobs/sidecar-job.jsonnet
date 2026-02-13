{
    name: "sidecar-example",
    tasks: [
        {
            name: "api",
            kind: "exec",
            lifecycle: "service",
            group: "app",
            config: {
                command: ["sleep", "120"],
            },
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
