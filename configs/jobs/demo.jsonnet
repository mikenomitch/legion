{
    name: "demo",
    tasks: {
        "first-task": {
            count: 1,
            driver: "noop",
            config: {
                message: "Hello from first task",
            },
            resources: {
                memory: 256,
            },
        },
        "second-task": {
            count: 2,
            driver: "noop",
            config: {
                message: "Hello from second task",
            },
            resources: {
                memory: 128,
            },
        },
    },
}
