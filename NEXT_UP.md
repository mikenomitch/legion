# Next Up

- Fix the store specs

- Eval Loop processes thing
- A new job sticks an eval on the queue
- It removes itself in its handle function
- Its handle function "places" it onto a "random node" with an alloc
- You can print out the ledger
- Set up some sort of TDD system and crank thru scheduling logic

  - Task placements/Job Start
  - Task replacement/Job Update
  - Node drops
  - Allocs dying

- On job submit, tasks are _syncronously_ placed onto nodes

  - A ledger is kept of all the node resources and whats on the nodes

    - All allocs
    - All nodes
    - You can return this via the CLI
      - "Node A, Resources: 10GB RAM, 10GB Disk, 10 CPU"
        - Alloc A
        - Alloc B
      - Node B ...etc...

  - Ledger CLI command prints it
  - Initially you can go over the resources (it just tracks the totals)
  - If you don't have enough resources on submit, the job
    command fails and tells you why!
  - Then figure out how to make it an async loop
  - Then respond to node changes with reschedules

- If you fail mid way thru any of the job submit parts, the transaction fails HARD!

- Submissions

  - Fix the cascade delete not working on submissions!
  - If you destroy a job, destroy its submissions if and only if they are not used by another job

- Figure out what to do about ids for job and job_config and whatnot
- Can pass a flag in to force bump job + deploy it (even if same submission)

- First party resource stuff + ledger? (start w/ allocs?)

  - I submit a "job" and it places on a node
  - I keep submitting jobs and they stop placing if resources run out
  - I can remove a job and it frees up resources

  - Plugin system for this
    - resources are just a special type of attr
    - tracked by the manager (not by the node itself)
    - Where do I set max_allocs?? (for an instance of a resouce)
      - I can add some constraint to the whole cluster?
        - I add a resource plugin to the cluster. It looks at attributes and then treats them as resource I can schedule on?
      - At the node level, I just have an attr of some sort.

# Other shovel ready work

- Nicer errors in CLI commands for various failures

  - Join token errors for instance (see To Dos)
  - Start with bad join token
  - Node info error (missing node)
  - Node update error
    - Node missing
  - Nice storage errors generally
    - I think I'll have to have different error types
      for the storage layer that the sql errors translate into
      and then the layer calling the store has to deal with these
      at a slightly different layer of abstraction
    - See this page for in-store error handling (https://users.rust-lang.org/t/handle-rusqlite-error/39660/7)

- Think about your file setup (names/org/dupes)

  - Split out NodeChecks into own single file then keep
    node check logic together in one
  - Where structs get their definition
  - Audit all files

- Write some integration tests
- Write some unit tests
- Any other plugins? (Storage maybe?)
- Automated testing in CI (timebox this)
- Common Rust crates (errors? anyhow?)

- Add otel tracing properly

- DRY up

  - Error handling
  - Store

- Worker configs CRUD commands with push from manager

  - I guess for this to really work, you would need to update any of this type
    that already exist (and send them the info)
  - Which means that nodes should save a reference to their type

- Fix the join-token and pre-register call destinations

  - Should the "insecure client" really just be a "public cert client"
    - Gets from lets encrypt by default but you can provide your own CA too?
  - Then the "client" is the "secure client" which does some self-signed CA magic
    and only the workers have access to it?
  - Or is TLS just not right right way to do this at all?
  - !! You need to plan this and not just code it!!

  - Environment variable or flag used for contacting manager
    - How to nicely join-server port from this?
      - You don't need it because its in the JWT!

# Milestone

- Hitting this point will be a fairly major milestone, as lots
  of node management stuff is "done"

# After that

- Goal: Scheduling a Job on a node for real
- Goal: Updating a job in response to node changes
- Goal: Replacing a job in response to health check failure
- Goal: Observing jobs on nodes via the CLI (where is it running, etc)

# Mid Term Goal (real personal use)

- I can run a manager/worker combo on Hetzner
- I can run docker images
- I can mount a host volume

### After that

- The app restarts if it crashes
- I can monitor app health via CLI and UI
- I can do deploys & updates
- I can have basic networking between tasks and split between two nodes
- Security stuff!!
- I can view logs (and they don't destroy my disk)
- I can shell into the containers
- I can monitor with a basic UI

# Deferred Work

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
