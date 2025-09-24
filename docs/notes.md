# Notes and Thoughts

### Scheduler Types
* Speed (Workers bid on it, workers are the source of truth, Fly.io Inspired)
* Bin-Packing (Manager selects, manager is source of truth)
* Spread (Manager selects, manager is source of truth)

### Built-in Task Kinds
* container
* executable (landlock, pledge, etc)
* wasm
* VM
* micro-VM container (for serverless containers)

### Changes

## TODOs:

Not exhaustive, but noting things so I remember:
* Figure out if plugin return type should change so its more language agnostic

## Deferred Work

### TLS joining

- Goal: Make join process establish TLS and/or WG

- Success:

* Run server with `cr start``
* Try `cr job status`
* Note that it fails
* Get a join-token
* Join with this token. Note that the client connection works.
* You should also be able to get a join-token for CLI certs

- Steps:

* Run worker start
