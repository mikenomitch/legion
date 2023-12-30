# Next Up
- First party resource stuff + ledger? (start w/ allocs?)
    - Plugin system for this
        - resources are just a special type of attr
        - tracked by the manager (not by the node itself)
        - Where do I set max_allocs?? (for an instance of a resouce)
            - I can add some constraint to the whole cluster?
                - I add a resource plugin to the cluster. It looks at attributes and then treats them as resource I can schedule on?
            - At the node level, I just have an attr of some sort.

- Clean up every unwrap()
- Clean up gross conversions of attr hashes
- Join token for a non existent node type is fine
- Node info for non-existent node errors with nice error

- Output node attrs as a map in json not a string
- Make many of the worker node attrs optional in the config
    - Nice errors for the ones that aren't
- Same for manager config

- Nice errors in CLI commands for various failures
    - Join token errors for instance (see To Dos)
- Better logging throughout
- Write some integration tests
- Write some unit tests
- Any other plugins? (Storage maybe?)
- Think about your file setup (names/org/dupes)
- Automated testing in CI (timebox this)
- Worker configs CRUD commands
    - I guess for this to really work, you would need to update any of this type
      that already exist (and send them the info)
    - Which means that nodes should save a reference to their type

# Milestone
* Hitting this point will be a fairly major milestone, as lots
of node management stuff is "done"

# After that
* Goal: Scheduling a Job on a node for real
* Goal: Updating a job in response to node changes
* Goal: Replacing a job in response to health check failure
* Goal: Observing jobs on nodes via the CLI (where is it running, etc)

# Mid Term Goal (real personal use)
* I can run a manager/worker combo on Hetzner
* I can run docker images
* I can mount a host volume
### After that
* The app restarts if it crashes
* I can monitor app health via CLI and UI
* I can do deploys & updates
* I can have basic networking between tasks and split between two nodes
* Security stuff!!
* I can view logs (and they don't destroy my disk)
* I can shell into the containers
* I can monitor with a basic UI


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
