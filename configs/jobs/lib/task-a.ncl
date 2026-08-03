{
    name = "task-a",
    runtime = "exec",
    config = {
        command = ["echo", "Hello from task A"],
    },
    resources = {
        memory = 256,
    },
    constraints = {},
    count = 1,
}
