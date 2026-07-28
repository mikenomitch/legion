function(job_name, cpu=100) {
    name: job_name,
    tasks: [
        {
            name: "variable-task",
            runtime: std.extVar("task_runtime"),
            count: std.parseInt(std.extVar("task_count")),
            config: {
                command: ["echo", std.extVar("message")],
            },
            resources: {
                cpu: cpu,
                memory: 128,
            },
            constraints: {},
            affinities: {},
        }
    ],
}
