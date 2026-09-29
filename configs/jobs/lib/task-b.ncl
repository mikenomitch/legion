{
    name = "task-b",
    runtime = "exec",
    config = {
        command = ["echo", "Hello from task B"],
    },
    resources = {
        memory = 256,
    },
    constraints = {},
    count = 2,
}
