# Agent sandboxes, created on demand by the launcher (tests/spot-check/launcher)
# or by hand. Submitting the job starts nothing: each sandbox is a keyed
# instance with its own persistent /workspace, created and destroyed by id.
#
#   legion job run tests/spot-check/jobs/07-agents.ncl
#   legion instance create agents --task box --id demo-1
#   legion instance get demo-1 -j               # currentAllocId once placed
#   legion exec <alloc-id> -it -- /bin/bash
#   legion instance destroy demo-1
#
# Two flavors, because they differ in what works today:
#
# - `vm`: a cloud-hypervisor microVM with its own kernel and a block volume.
#   The real isolation boundary for untrusted agents. Exec is NOT supported
#   on the microVM path yet (no guest agent), so on Linux you only get logs.
# - `box`: a container in a `gated` network namespace. Its only way out is the
#   node's egress proxy, which allows package registries and GitHub and denies
#   everything else. Exec works.
let images = (import "lib/images.ncl") vars in
{
  name = "agents",
  tasks = [
    {
      name = "vm",
      group = "vm",
      runtime = "sandbox",
      stateful = {
        mode = "keyed",
        storage = { path = "/workspace", mode = "block", params = { size = "1G" } },
      },
      config = {
        image = images.agent,
        command = ["/usr/local/bin/agent"],
        env = { AGENT_TASK = "refactor the payments module" },
        vcpus = 1,
        memory_mib = 512,
        hypervisor = "cloud-hypervisor",
      },
      resources = { cpu = 1000, memory = 512 },
      constraints = { os = "linux" },
    },
    {
      name = "box",
      group = "box",
      runtime = "container",
      stateful = {
        mode = "keyed",
        storage = { path = "/workspace", driver = "local-dir", params = { size = "1G" } },
      },
      config = {
        image = images.agent,
        command = ["/usr/local/bin/agent"],
        env = { AGENT_TASK = "write tests for the CLI" },
      },
      network = {
        mode = "gated",
        labels = { tier = "agent" },
        egress = {
          protocol = "http",
          policy = "deny",
          allow = ["pypi.org", "files.pythonhosted.org", "github.com", "codeload.github.com", "api.github.com"],
          accessLog = true,
        },
      },
      resources = { cpu = 500, memory = 256 },
    },
  ],
}
