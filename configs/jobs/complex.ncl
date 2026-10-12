{
    name = "demo",
    tasks = {
        "first-task" = {
            count = 1,
            runtime = "exec",
            config = {
                command = ["sleep", "1"],
            },
            resources = {
                cpu = 100,
                memory = 256,
            },
        },
        "second-task" = {
            count = 2,
            runtime = "exec",
            config = {
                command = ["sleep", "2"],
            },
            resources = {
                cpu = 500,
                memory = 128,
            },
            constraints = [
                {
                    key = "placedWith",
                    value = "first-task",
                },
                {
                    key = "worker",
                    operator = "distinct",
                },
                {
                    key = "cpu",
                    value = "amd",
                },
            ],
            dependsOn = [
                {
                    task = "first-task",
                    condition = "start",
                },
            ],
        },
    },
}
