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
  - [ ] It can be reconfigured from the server
  - [ ] It can be restarted from the server - Codex
  - [ ] TLS is autoconfigured and auto-rotated

- [ ] Scheduling
  - [x] Ledger command. I can see all the nodes and what is on them easily via a CLI command. - Codexed
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
  - [x] Rescheduling works
    - [x] Health checks trigger reschedule (after restart)
  - [ ] Dynamic metadata
    - [ ] A node attr change will update a job's placement
  - [x] Job health check reschedules a job
    - [x] Initial failures restart the job
    - [x] Failure after N times forces a reschedule

- [ ] Running workloads
  - [x] A framework is set up to run workload plugins
  - [ ] A "noop" kind "works" - Test
  - [ ] An "exec" kind works - Test
    - [ ] The command and args given are run
    - [ ] It runs as a specific user
    - [ ] Unveil works properly
    - [ ] It is put in a cgroup that restricts cpu, memory, and disk appropriately
    - [ ] It uses landlock appropriately
    - [ ] It handles network namespaces well?
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
  - [ ] Saving the SQLite works (litestream-like)?
  - [ ] Raft Works
 
- [ ] Stateful support
  - [ ] ???
  
- [ ] Short-lived jobs
  - [ ] Basic short-lived works
  - [ ] DAGS (different machines)

- [ ] Task groups and dependencies
  - [ ] Can place two on the same machine
  - [ ] DAG (same machines)
  
- [ ] Cron jobs
- [ ] System Job
- [ ] Sysbatch Job

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

- [ ] Joining a cluster and TLS is very easy

## Long Term

- [ ] A "speed" scheduler works that optimizes for fast placement
    - [ ] Uses Fly.io bidding architecture
    - [ ] Workers start pre-fetching before being notified they have won
    - [ ] Submitter can set a time or number of submissions before selecting winner
    
- [ ] A "VM" kind works
  - Uses Libvirt? Uses cloud-hypervisor? QEMU?

- [ ] ACL System

- [ ] Workload identity tokens
