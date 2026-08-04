{
    name = "dag-same-group",
    tasks = [
        {
            name = "build",
            runtime = "exec",
            lifecycle = "batch",
            group = "app",
            config = {
                command = ["echo", "build"],
            },
        },
        {
            name = "api",
            runtime = "exec",
            lifecycle = "service",
            group = "app",
            dependsOn = [
                {
                    task = "build",
                    condition = "success",
                },
            ],
            config = {
                command = ["echo", "api"],
            },
        },
    ],
}
