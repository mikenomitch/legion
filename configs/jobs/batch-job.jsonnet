{
    name: "batch-example",
    tasks: [
        {
            name: "migrate",
            kind: "exec",
            lifecycle: "batch",
            group: "db",
            config: {
                command: ["echo", "run", "migrations"],
            },
        },
    ],
}
