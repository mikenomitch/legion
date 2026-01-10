function(job_name, cpu=100) {
    name: job_name,
    tasks: [
        {
            name: "variable-task",
            kind: std.extVar("task_kind"),
            count: std.parseInt(std.extVar("task_count")),
            config: {
                message: std.extVar("message"),
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
