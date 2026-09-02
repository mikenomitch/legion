{
    name = "batch-example",
    tasks = [
        {
            name = "migrate",
            runtime = "exec",
            lifecycle = "batch",
            group = "db",
            config = {
                command = ["echo", "run", "migrations"],
            },
        },
    ],
}
