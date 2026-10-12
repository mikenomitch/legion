# The smallest possible placement probe: one exec task that sleeps. Use it to
# ask "can this cluster place anything at all right now?"
#
#   legion job run tests/spot-check/jobs/00-probe.ncl && legion job logs probe
{
  name = "probe",
  task = {
    name = "sleep",
    runtime = "exec",
    lifecycle = "service",
    config = { command = ["sh", "-c", "echo probe placed on $(hostname); exec sleep 3600"] },
    resources = { cpu = 50, memory = 16 },
  },
}
