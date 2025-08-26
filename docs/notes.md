# Notes and Thoughts

### Scheduler Types
* Speed (Workers bid on it, workers are the source of truth, Fly.io Inspired)
* Bin-Packing (Manager selects, manager is source of truth)
* Spread (Manager selects, manager is source of truth)

### Built-in Task Types
* container
* executable (landlock, pledge, etc)
* wasm
* VM
* micro-VM container (for serverless containers)

### Changes