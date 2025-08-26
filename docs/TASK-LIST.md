# Task List and Milestones
- [x] Start a manager node
  - [x] start starts a web server
    - [x] Tonic for gRPC

- [x] Add a follower node
  - [x] CLI arg to get address
  - [x] Makes contact via RPC
  - [x] Add message passing between the two via RPC
  - [x] It begins to heartbeat. Is marked unhealthy if it misses.
    - [ ] Be able to watch this change via the CLI
  - [x] It has a basic fingerprinting system
  - [ ] It has dynamic attributes that can be updated (I think this is done?)
  - [ ] It properly shows up in the manager's ledger of worker nodes (is this done?)
  - [ ] It can be reconfigured from the server
  - [ ] It can be restarted from the server
  - [ ] TLS is autoconfigured and auto-rotated

- [x] Job submission
  - [x] CLI can submit a “job” object
  - [x] GRPC call to submit a job
  - [x] Job is parsed
  - [x] Job is stores in the backend

- [ ] Scheduling
  - [ ] Ledger command. I can see all the nodes and what is on them easily via a CLI command.
  - [ ] A framework for running scheduler plugins exiss
  - [ ] A "random" scheduler that selects nodes at random until one works, "works"
  - [ ] Constraints work
  - [ ] A "bin-pack" scheduler works
  - [ ] Affinities and anti-affinities work
  - [ ] Scheduling on same node as existing job works
  - [ ] Priorities and eviction works (eviction toggled at the pool level)
  - [ ] A "spread" scheduler works (inverse of bin-pack)
  - [ ] A "speed" scheduler works that optimizes for fast placement
    - [ ] Uses Fly.io bidding architecture
    - [ ] Workers start pre-fetching before being notified they have won
    - [ ] Submitter can set a time or number of submissions before selecting winner

- [ ] Running workloads
  - [ ] A framework is set up to run workload plugins
  - [ ] A "noop" type "works"
  - [ ] An "executable" type works
    - [ ] The command and args given are run
    - [ ] It runs as a specific user
    - [ ] Unveil works properly
    - [ ] It is put in a cgroup that restricts cpu, memory, and disk appropriately
    - [ ] It uses landlock appropriately
    - [ ] It handles network namespaces well?
  - [ ] A "container" type works
    - Does it just user docker? Apple's container' runtime if on MacOS?
    - MUST work on a Mac easily (and on linux)
        1. Think about Kamal
        2. Think about local dev
  - [ ] A "WASM" type works
  - [ ] A "VM" type works
    - Uses Libvirt? Uses cloud-hypervisor?

- [ ] Deployments
  - [ ] A new version of a job starts a deployment
  - [ ] A framework is set up for deployments
  - [ ] An "immediate" deployment type "works" and cycles out everything in place automatically
  - [ ] A "rolling" deployment type is created and works
