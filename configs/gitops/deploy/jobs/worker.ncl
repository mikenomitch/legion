let common = import "../lib/common.ncl" in
{
  name = "worker",
  task = {
    name = "worker",
    runtime = "container",
    config = { image = "busybox:1.36", command = ["sh", "-c", "while true; do echo working; sleep 30; done"] },
    resources = common.resources,
  },
}
