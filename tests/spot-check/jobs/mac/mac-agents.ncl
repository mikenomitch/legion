# Agent sandboxes on macOS. The sandbox runtime runs in dev mode on Apple
# `container` (each sandbox is its own VM), which supports exec, so the
# launcher's terminal works end to end here.
#
#   container build -t spot-check-agent:dev tests/spot-check/images/agent
#   legion job run tests/spot-check/jobs/mac/mac-agents.ncl
#   LAUNCHER_JOB=mac-agents python3 tests/spot-check/launcher/launcher.py
#
# Dev mode needs filesystem volumes (it bind-mounts), not block volumes.
{
  name = "mac-agents",
  tasks = [
    {
      name = "vm",
      runtime = "sandbox",
      stateful = {
        mode = "keyed",
        storage = { path = "/workspace", driver = "local-dir" },
      },
      config = {
        image = "spot-check-agent:dev",
        command = ["/usr/local/bin/agent"],
        env = { AGENT_TASK = "summarize the changelog" },
        memory_mib = 512,
      },
      resources = { memory = 512 },
      constraints = { os = "macos" },
    },
  ],
}
