# A job that sets the example gitops app's jobspec keys
# (configs/apps/example-app.ncl; docs/apps.md, "Jobspec extensions").
#
# It submits only where that app is installed: elsewhere `gitops` and
# `syncWave` are unknown keys (or, with --allow-unknown-fields, pending
# values the app adopts when it is installed).
#
#   legion app install configs/apps/example-app.ncl
#   legion job validate configs/apps/example-app-job.ncl   # checks the values against the app's schemas
#   legion job run configs/apps/example-app-job.ncl
#   legion job info storefront                             # each value, with its defaults, and its state
{
  name = "storefront",
  # The job point: the app's `gitops` key, next to what it describes.
  # `autoSync` is left out: it reads as the schema's default (true).
  gitops = {
    repo = "https://github.com/acme/deploy",
    path = "jobs/storefront.ncl",
  },
  tasks = [
    {
      name = "web",
      group = "frontend",
      runtime = "exec",
      config = { command = ["sleep", "3600"] },
      resources = { cpu = 100, memory = 64 },
      # The group point, written on one task of the group: the whole
      # `frontend` group syncs in wave 1. Setting it needs template:write
      # (the key's `requires`).
      syncWave = { wave = 1 },
    },
    {
      name = "worker",
      group = "background",
      runtime = "exec",
      config = { command = ["sleep", "3600"] },
      resources = { cpu = 100, memory = 64 },
      # The qualified spelling, which works at every point and never
      # collides with a core field.
      apps = { gitops = { wave = 2 } },
    },
  ],
}
