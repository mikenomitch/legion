# Next Up
- Ledger prints nicely - Codexed
    - get_ledger updated and GetLedgerReply updated
    - prints out a list of all the nodes with allocations on them
    - CLI will properly and nicely print out the ledger

- The Eval loop is being treated incorrectly. Think this thru.
  It is not about adding nodes/jobs. It is about scheduling these
  jobs once they are there. Or responding to the changes in nodes.

  Does it go and construct some new data thats a minimular ledger,
  and then this gets picked up by some loop that goes and sends
  signals to the nodes to then go do stuff?

  Or is it sending the signals to the nodes itself?
  Or do the nodes check in and ask it for information on what to do
  when they heartbeat (prob not!).

- Eval loop only goes thru nodes and jobs and calls the scheduler
  The nodes and jobs have already been added to the list. The eval
  loop just has to process changes and update the ledger accordingly
  and also send the signals to the nodes to actually do stuff properly.

  So the state in the DB of jobs should reflect reality at any given point.

  A node_join eval -> Goes and sees if any jobs are waiting on that node
  A node_drop eval -> Goes and reschedules jobs from that node
  A job_submit eval -> Goes and schedules it onto a node if its not on one already
  A job_update eval -> Goes and schedules it onto a node if its not on one already. Updates if needed
  A job_drop eval -> Removes it from the nodes.

- Eval Loop can process things
    -  Start with a test of handling a chain of evals via Eval::handle being called repeatedly on them:
        - Start with empty ledger
        - Add a node via node_join
        - Ledger shows an empty node
        - Add another node via node_join
        - Ledger shows two empty nodes
        - Drop a node via node_drop
        - Ledger shows an empty node
        - Add a job via job_submit
        - Allocations are added to the node
        - Update the job via job_update
        - Allocations are updated on the node (not added)
        - Add a second job via job_submit
        - Allocations are added to the node
        - Remove a job via job_stop
        - Allocations are removed from the node
        - Remove the second job via job_stop
        - All allocations are removed from the node and the ledger is empty

        - Ensure that after each eval is handled by the store, it removes itself from the store

- Eval Loop processes thing
- A new job sticks an eval on the queue
- It removes itself in its handle function
- Its handle function "places" it onto a "random node" with an alloc
- Set up some sort of TDD system and crank thru scheduling logic

- The Eval loop will clean up future evaluations having to do with same job or
  node since it now will have processed them

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
  - Then figure out how to make it an optimistic async loop
  - Then respond to node changes with reschedules

- If you fail mid way thru any of the job submit parts, the transaction fails HARD! - Codexed

- Submissions - Codexed
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
  - Where structs get their definition
  - Audit all files

- Write some integration tests
- Write some unit tests
- Any other plugins?
  - Storage?
  - Config?

- Add otel tracing properly?

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
