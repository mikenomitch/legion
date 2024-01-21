local taskA = import 'task-a.libsonnet';
local taskB = import 'task-b.libsonnet';

{
    name: "another-imports-example",
    tasks: [
        taskA,
        taskB,
    ]
}
