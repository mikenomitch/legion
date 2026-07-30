// Worker type for the simulated fleets. Join tokens are minted against
// it (`legion join-token create --type sim`); the sim processes
// pre-register with it and take the pool from here. Installed into every
// manager's workerConfigPath by the install scripts.
{
  workerType: 'sim',
  pool: 'default',
}
