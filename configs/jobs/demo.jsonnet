{
    name: "demo",
    tasks: [
        {
            name: "first-task",
            driver: "docker",
            config: {
                message: "Hello from first task",
            },
            resources: {
                memory: 256,
            },
            constraints: {},
            count: 1,
        },
        {
            name: "second-task",
            driver: "noop",
            config: {
                message: "Hello from second task",
            },
            resources: {
                memory: 128,
            },
            constraints: {},
            count: 2,
        }
    ]
}
