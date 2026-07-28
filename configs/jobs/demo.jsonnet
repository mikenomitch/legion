{
    name: "demo5",
    tasks: [
        {
            name: "first-task",
            runtime: "exec",
            config: {
                command: ["echo", "hello", "from", "first task"],
            },
            resources: {
                memory: 256,
            },
            constraints: {},
            count: 3,
        },
        {
            name: "second-task",
            runtime: "exec",
            config: {
                command: ["echo", "hello", "from", "second task"],
            },
            resources: {
                memory: 128,
            },
            constraints: {},
            count: 2,
        }
    ]
}
