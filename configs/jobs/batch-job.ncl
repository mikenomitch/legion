# A run-to-completion job. A `batch` task finishes when its process exits 0,
# and is retried in place when it fails.
#
#   legion job run configs/jobs/batch-job.ncl
#   legion job logs batch-example
{
  name = "batch-example",
  tasks = [
    {
      name = "migrate",
      runtime = "exec",
      lifecycle = "batch",
      group = "db",
      config = {
        command = ["sh", "-c", "echo running migrations; sleep 2; echo done"],
      },
      # Batch tasks default to `onFailure`. Exit code 3 means "nothing to do",
      # so count it as success too.
      restart = { policy = "onFailure", delay = "10s", successExitCodes = [0, 3] },
      resources = { cpus = 0.1, memory = 64 },
    },
  ],
}
