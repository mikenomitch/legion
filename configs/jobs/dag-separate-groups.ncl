# A task graph across three groups. Batch prep work fans out and back in, and
# the services start only once their dependencies are satisfied:
#
#   migrate ──► seed ────────┐
#           └─► build-assets ┴──► api ──► web
#                                  └──► log-tail (sidecar)
#
# Conditions: `success` waits for a batch task to exit 0. `start` only waits
# for the dependency to start, which is the most a service can promise.
#
#   legion job run configs/jobs/dag-separate-groups.ncl
#   legion job info dag-separate-groups
let dep = fun on_task on_condition => { task = on_task, condition = on_condition } in
{
  name = "dag-separate-groups",
  tasks = [
    {
      name = "migrate",
      runtime = "exec",
      lifecycle = "batch",
      group = "prep",
      config = { command = ["sh", "-c", "echo migrating; sleep 2"] },
    },
    # `seed` and `build-assets` both wait on `migrate`, then run in parallel.
    {
      name = "seed",
      runtime = "exec",
      lifecycle = "batch",
      group = "prep",
      dependsOn = [dep "migrate" "success"],
      config = { command = ["sh", "-c", "echo seeding; sleep 2"] },
    },
    {
      name = "build-assets",
      runtime = "exec",
      lifecycle = "batch",
      group = "prep",
      dependsOn = [dep "migrate" "success"],
      config = { command = ["sh", "-c", "echo building assets; sleep 3"] },
    },
    # Cross-group: the api waits on both prep branches.
    {
      name = "api",
      runtime = "exec",
      lifecycle = "service",
      group = "backend",
      dependsOn = [dep "seed" "success", dep "build-assets" "success"],
      config = { command = ["sh", "-c", "echo api up; sleep 3600"] },
    },
    # Sidecars may only depend on tasks in their own group.
    {
      name = "log-tail",
      runtime = "exec",
      lifecycle = "sidecar",
      group = "backend",
      dependsOn = [dep "api" "start"],
      config = { command = ["sh", "-c", "echo tailing api logs; sleep 3600"] },
    },
    {
      name = "web",
      runtime = "exec",
      lifecycle = "service",
      group = "frontend",
      dependsOn = [dep "api" "start"],
      config = { command = ["sh", "-c", "echo web up; sleep 3600"] },
    },
  ],
}
