let taskA = import "lib/task-a.ncl" in
let taskB = import "lib/task-b.ncl" in

{
    name = "another-imports-example",
    tasks = [
        taskA,
        taskB,
    ]
}
