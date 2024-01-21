{
    name: "task-b",
    driver: "docker",
    config: {
        message: "Hello from task B",
    },
    resources: {
        memory: 256,
    },
    constraints: {},
    count: 2,
}