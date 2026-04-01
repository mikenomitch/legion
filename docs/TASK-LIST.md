# Task List and Milestones
- [ ] Worker Nodes
  - [x] Add a Worker node
  - [x] CLI arg to get address
  - [x] Makes contact via RPC
  - [x] Add message passing between the two via RPC
  - [x] It begins to heartbeat. Is marked unhealthy if it misses.
    - [x] Be able to watch this change via the CLI
  - [x] It has a basic fingerprinting system
  - [x] It has dynamic attributes that can be updated (I think this is done?) - Codexed
  - [x] It properly shows up in the manager's ledger of worker nodes (is this done?)
  - [ ] It can be reconfigured from the server using CRUD commands
    - [ ] For this to really work, do you need to update any of this type that already exist (and send them the info)? (Which means that nodes should save a reference to their type)?
  - [x] It can be restarted from the server - Test it
  - [ ] TLS is autoconfigured and auto-rotated

- [ ] Scheduling
  - [x] Ledger command. I can see all the nodes and what is on them easily via a CLI command.
  - [x] A framework for running scheduler plugins exists
  - [x] A "random" scheduler that selects nodes at random until one works, "works"
  - [x] Constraints work
  - [x] A "bin-pack" scheduler works
  - [x] A "spread" scheduler works (inverse of bin-pack)
  - [x] Affinities and anti-affinities work
  - [ ] Some task group equivalent works
  - [ ] Worker groups + taint/toleration equiv
  - [ ] Scheduling on same node as existing job works
  - [ ] Priorities and eviction works (eviction toggled at the pool level)
    - [ ] At least think about fair scheduling policies (do a lil LLMing to figure out if there's some 80/20 you can do here)
  - [x] Rescheduling works
    - [x] Health checks trigger reschedule (after restart)
  - [ ] Dynamic metadata
    - [ ] A node attr change will update a job's placement
  - [x] Job health check reschedules a job
    - [x] Initial failures restart the job
    - [x] Failure after N times forces a reschedule

- [ ] Observability & Perf
  - [ ] Add tracing to the scheduler
  - [ ] Add a way to nicely view traces in the UI (make dev only one day)
  - [ ] Write some benchmarks in a dedicated directory
  - [ ] Fix the slowest bits of the scheduler using the perf tests
  - [ ] Add to Agents.md that any changes to the scheduler should come with a before and after perf test benchmark

- [ ] Running workloads
  - [x] A framework is set up to run workload plugins
  - [ ] A "noop" kind "works" - Test
  - [x] An "exec" kind works - Test
    - [x] The command and args given are run
    - [x] It runs as a specific user
    - [ ] Unveil works properly
    - [x] It can join an existing cgroup (v2 only)
    - [x] It uses landlock appropriately
    - [ ] It handles network namespaces well? (use netavark lib?)
  - [ ] A "container" kind works
    - Does it just user docker? Apple's container' runtime if on MacOS?
    - MUST work on a Mac easily (and on linux)
        1. Think about Kamal
        2. Think about local dev
  - [ ] A "WASM" kind works
 
- [ ] Make something for the AIs to use this
  - Self-documenting CLI tool for agents
  - Everything has an easily accessible type and spec
  
- [ ] Artifact system
  - [ ] Able to grab docker images
  - [ ] Able to do some auth for this
  - [ ] Able to grab files and put them on disk
  - [ ] Have plugin system for this

- [ ] Rollouts
  - [ ] A new version of a job starts a deployment
  - [ ] A framework is set up for deployments
  - [ ] An "immediate" deployment kind "works" and cycles out everything in place automatically
  - [ ] A "rolling" deployment kind is created and works
  - [ ] A "blue-green" deployment kind is created and works
  - [ ] A "canary" deployment kind is created and works
  - [ ] Nice UI for this
  
- [ ] KV & Secrets
  - [ ] I can get one of these from a task
  - [ ] When I update one of these, the task restarts (if configured to do so)
  - [ ] I can use one of these in the defintion

- [ ] Service Discovery
  - [ ] I can declare a named service on a task
  - [ ] I can declare health checks on that task
  - [ ] Can query address and port from other services
  - [ ] I can declare which other jobs have access to my service
  
- [ ] Persisting state
  - [x] Saving the SQLite works (litestream-like)?
  - [x] Raft Works (leader election/failover controls, observability, CLI/UI ops)
 
- [ ] Stateful support
  - [ ] ???
  
- [ ] Short-lived jobs
  - [ ] Basic short-lived works
  - [ ] DAGS (different machines)

- [ ] Task groups and dependencies
  - [ ] Can place two on the same machine
  - [ ] DAG (same machines)
  
- [ ] Cron jobs
- [ ] Sysbatch Job
- [ ] System Job (N per X?) (1 per metal) (2 per region) (etc)
  - Is this more of a count issue than a job type itself??

- [ ] Operational Stuff
  - [ ] Joining a cluster
  - [ ] Losing a manager
  - [ ] Loading state back in from a file
  - [ ] Cluster federation
  - [ ] OTEL
  - [ ] Logging
  - [ ] Metrics
  - [ ] Event Stream

- [ ] Some Pack equivalent
  - [ ] There is a default registry
  - [ ] I can add a registry
  - [ ] I can call to run with @<registry>/<job>
  - [ ] I can inspect a job in the registry (just reads some metadata)
  - [ ] There is a UI for inserting variables from job defintions with metadata in them
  - [ ] Plugins can be fetched by URL or the registry

- [ ] Joining a cluster and TLS is very easy
- [ ] Fix the join-token and pre-register call destinations
  - [ ] Should the "insecure client" really just be a "public cert client"
    - [ ] Gets from lets encrypt by default but you can provide your own CA too?
  - [ ] Then the "client" is the "secure client" which does some self-signed CA magic
    and only the workers have access to it?
  - [ ] Or is TLS just not right right way to do this at all?
  - [ ] !! You need to plan this and not just code it!!

  - [ ] Environment variable or flag used for contacting manager
    - [ ] How to nicely join-server port from this?
      - [ ] You don't need it because its in the JWT!

- [ ] Audit EVERY name decision in depth. Important.

- [ ] AI Agent Stuff
  - [ ] Plugin with MCP server
  - [ ] Think thru Agents.md 
  - [ ] Self-documenting CLI so agents can use it nicelu
  - [ ] grpc endpoints very nicely documented (and auto-updating)
  - [ ] An AI agents should be able to very easily write a plugin
  - [ ] An AI agent should be able to very easily configure a Worker Node to talk to a manager node
  - [ ] An AI agent should be able to search the registry and find any jobs that it makes sense to install and run for you

## Long Term

- [ ] A "speed" scheduler works that optimizes for fast placement
    - [ ] Uses Fly.io bidding architecture
    - [ ] Workers start pre-fetching before being notified they have won
    - [ ] Submitter can set a time or number of submissions before selecting winner
    
- [ ] A "VM" kind works
  - Uses Libvirt? Uses cloud-hypervisor? QEMU?

- [ ] ACL System

- [ ] Workload identity tokens
