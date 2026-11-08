# A job gitops deploys from this repository (Repo `path = "jobs"`). Change
# the image tag, commit, and push: gitops rolls it out within its interval,
# or at once with `legion gitops sync`.
let common = import "../lib/common.ncl" in
{
  name = "web",
  task = {
    name = "server",
    runtime = "container",
    count = 2,
    config = { image = "nginx:1.27-alpine" },
    resources = common.resources,
    network = { ports = [{ name = "http", to = 80 }] },
  },
}
