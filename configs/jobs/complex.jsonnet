{
    name: "demo",
    tasks: {
        "first-task": {
            count: 1,
            kind: "noop",
            config: {
                message: "Hello from first task",
            },
            resources: {
                cpus: 0.1,
                memory: 256,
            },
        },
        "second-task": {
            count: 2,
            kind: "noop",
            config: {
                message: "Hello from second task",
            },
            resources: {
                cpus: 0.5,
                memory: 128,
            },
            constraints: [
                ["placedWith", "first-task"],
                ["worker", "UNIQUE"],
                ["cpu", "amd"],
            ],
            starts:{
                onStart: "first-task"
            },
            ends: {
                onComplete: "first-task",
            },
        },
    },
}
