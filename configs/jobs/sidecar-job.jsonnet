{
    name: "sidecar-example",
    tasks: [
        {
            name: "api",
            runtime: "exec",
            lifecycle: "service",
            group: "app",
            config: {
                command: ["sleep", "120"],
            },
        },
        {
            name: "logger",
            runtime: "exec",
            lifecycle: "sidecar",
            group: "app",
            config: {
                command: ["echo", "tail", "logs"],
            },
        },
    ],
}
