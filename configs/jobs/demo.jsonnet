{
    name: "demo5",
    tasks: [
        {
            name: "first-task",
            driver: "container",
            config: {
                message: "Hello from first task",
            },
            resources: {
                memory: 256,
            },
            constraints: {},
            count: 3,
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
