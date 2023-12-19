# Next Up
* Goal: Add node type on join-token

* Success: I can make a new node type and save it
- Fix the type errors when loading attrs
- Get attrs saving properly on Node Join
- I can join "default" and "alt" and properly see pool and attrs
  threading thru all the way to the node info command & node list commands

# Next Up
* Goal: Add expiry on join-tokens

* Success:
- I can pass an argument into the join-token generation for unlimited or a time
- It defaults to 1 hour use
- The JWT has this auto encoded in it and this is validated on join

# After that
- Other health status checks along with heartbeats?
    - Like heartbeating but at slower cadence (jsonb saved and pinged every N seconds)
    - Plugin system like fingerprinters
- First party resource stuff + ledger? (start w/ allocs?)
    - Plugin system for this

- Clean up every unwrap()
- Clean up gross conversions of attr hashes

- Output node attrs as a map in json not a string
- Make many of the worker node attrs optional in the config
    - Nice errors for the ones that aren't
- Same for manager config

- Nice errors in CLI commands for various failures
- Better logging throughout
- Write some integration tests
- Think about your file setup (names/org/dupes)
- Automated testing in CI (timebox this)

# Milestone
* Hitting this point will be a fairly major milestone, as lots
of node management stuff is "done"

# After that
* Goal: Scheduling a Job on a node for real
* Goal: Updating a job in response to node changes
* Goal: Replacing a job in response to health check failure
* Goal: Observing jobs on nodes via the CLI (where is it running, etc)

# Deferred Work

### TLS joining
* Goal: Make join process establish TLS and/or WG

* Success:
- Run server with `cr start``
- Try `cr job status`
- Note that it fails
- Get a join-token
- Join with this token. Note that the client connection works.
- You should also be able to get a join-token for CLI certs

* Steps:
- Run worker start
