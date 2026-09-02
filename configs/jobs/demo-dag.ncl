# A multi-stage data pipeline DAG for showcasing the UI visualization.
#
#   bootstrap ──► extract ──► validate ─┐
#             └─► fetch   ──► transform ─┤──► aggregate ──► publish
#                       └► audit (sidecar)             └──► notify
#
# Long-running "service" tasks stay healthy so the graph shows live status;
# "batch" stages complete quickly to unblock their successors.
#
# Nickel records are recursive, so a field always refers to its own record
# first — writing `{ name = name }` inside `fun name => …` is a self-reference,
# not the argument. Hence the `task_` prefixes on the parameters below.
let service = fun task_name task_group task_deps => {
  name = task_name,
  runtime = "exec",
  lifecycle = "service",
  group = task_group,
  dependsOn = task_deps,
  config = { command = ["sleep", "3600"] },
  resources = { cpus = 0.1, memory = 128 },
}
in

let batch = fun task_name task_group seconds task_deps => {
  name = task_name,
  runtime = "exec",
  lifecycle = "batch",
  group = task_group,
  dependsOn = task_deps,
  config = { command = ["sleep", std.string.from_number seconds] },
  resources = { cpus = 0.1, memory = 128 },
}
in

let sidecar = fun task_name task_group task_deps => {
  name = task_name,
  runtime = "exec",
  lifecycle = "sidecar",
  group = task_group,
  dependsOn = task_deps,
  config = { command = ["sleep", "3600"] },
  resources = { cpus = 0.1, memory = 64 },
}
in

let dep = fun on_task on_condition => { task = on_task, condition = on_condition }
in

{
  name = "demo-dag",
  tasks = [
    # Stage 1 — entrypoint
    batch "bootstrap" "bootstrap" 2 [],

    # Stage 2 — fan out from bootstrap
    batch "extract" "ingest" 3 [dep "bootstrap" "success"],
    batch "fetch" "ingest" 3 [dep "bootstrap" "success"],
    sidecar "audit" "ingest" [dep "extract" "start"],

    # Stage 3 — long-running processors
    service "validate" "process" [dep "extract" "success"],
    service "transform" "process" [dep "fetch" "success"],

    # Stage 4 — fan in (join two branches)
    service "aggregate" "process" [dep "validate" "start", dep "transform" "start"],

    # Stage 5 — outputs
    service "publish" "output" [dep "aggregate" "start"],
    batch "notify" "output" 8 [dep "aggregate" "start"],
  ],
}
