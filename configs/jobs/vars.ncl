# Parameterized jobspec (docs/jobs-tasks-and-scheduling.md, "Variables").
#
# Every jobspec is evaluated with a free `vars` record in scope, built from the
# `--var` / `--var-code` / `--kv-var` / `--kv-var-code` flags:
#
#   legion job run configs/jobs/vars.ncl \
#     --var job_name=variable-demo \
#     --var message=hello \
#     --var task_runtime=exec \
#     --var-code task_count=3 \
#     --var-code cpu=250
#
# `--var` always delivers a string; `--var-code` evaluates a Nickel expression,
# so numbers stay numbers and booleans stay booleans. Merging `vars` over a
# record of defaults is the idiomatic way to make a variable optional — the
# `default` annotation loses to anything the caller supplied.
let params =
  {
    cpu | Number | default = 100,
    task_count | Number | default = 1,
  }
  & vars
in
{
  name = params.job_name,
  tasks = [
    {
      name = "variable-task",
      runtime = params.task_runtime,
      count = params.task_count,
      config = {
        command = ["echo", params.message],
      },
      resources = {
        cpu = params.cpu,
        memory = 128,
      },
      constraints = {},
      affinities = {},
    }
  ],
}
