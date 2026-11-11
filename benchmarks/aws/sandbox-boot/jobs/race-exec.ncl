# A keyed host-process task for the ratification race (race.sh): every node
# starts the same named instance at once, and exactly one may keep it.
{
  name = "race",
  tasks = [{
    name = "box",
    runtime = "exec",
    config = { command = ["/bin/sleep", "3600"] },
    resources = { cpu = 10, memory = 16 },
    stateful = { mode = "keyed" },
    # A host process has no network gate to hold shut, so it serves at once.
    speculativeStart = { mode = "serve", ratifyTimeout = "10s" },
    constraints = { os = "linux" },
  }],
}
