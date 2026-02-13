{
    name: "task-a",
    kind: "exec",
    config: {
        command: ["echo", "Hello from task A"],
    },
    resources: {
        memory: 256,
    },
    constraints: {},
    count: 1,
}
