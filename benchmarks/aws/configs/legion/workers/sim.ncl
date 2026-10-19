# Worker type for the simulated fleets. Join tokens are minted against
# it (`legion join-token create --type sim`); the sim processes
# pre-register with it and take the pool from here. Installed into every
# manager's workerConfigPath by the install scripts.
#
# `workerFleet` grants the fleets the sim processes register as
# (`--name-prefix sim-<idx>`, and the sandbox campaign's loadgen fleet
# `sbx`): without it the manager refuses every simulated node
# (docs/worker-simulation.md, "One fleet certificate"). Only simulators join
# with this type, so it grants any fleet name.
{
  workerType = "sim",
  pool = "default",
  attributes = { workerFleet = "*" },
}
