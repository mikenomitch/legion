# Old Legion notes

These are old notes from Bear transferred to this repo for tracking

## OLD NEXT UP List
* CLI Libs
  * Read this: [Output for humans and machines - Command Line Applications in Rust](https://rust-cli.github.io/book/tutorial/output.html)

*Random Libs
* https://github.com/ratatui-org/ratatui
* https://github.com/gyscos/cursive
* https://github.com/zhiburt/tabled
* https://github.com/rust-cli/human-panic
* https://github.com/rust-unofficial/awesome-rust

- [ ] Implement OpenRaft https://github.com/datafuselabs/openraft
  - [ ] First node comes up as a single node raft cluster
    - [ ] Send an init RPC to leader
    - [ ] Handle RPCs at leader level
      - [ ] [openraft/examples/raft-kv-rocksdb/src/network/management.rs at 4b78dd38b9f3b22d87695fb06446dcf0311629c2 · datafuselabs/openraft](https://github.com/datafuselabs/openraft/blob/4b78dd38b9f3b22d87695fb06446dcf0311629c2/examples/raft-kv-rocksdb/src/network/management.rs#L47C6-L47C6)
      - [ ] 
    - [ ] You can write to raft (job run)
    - [ ] You can read from raft (job status)
  - [ ] Two followers can be added

## OLD Milestones
- [x] Start a server
  - [x] start starts a web server
    - [x] Tonic for gRPC https://github.com/hyperium/tonic
  - [x] start can be given an arg to start in follower mode

- [ ] Raft control nodes with distributed state
  - [x] Import raft
  - [ ] Create a Node struct and impl
    - [ ] Use MemStorage for now
      - [x] Configure Node with a single arg (to allow for many soon)
      - [x] Extract drive raft function based on [The TiKV blog | Implement Raft in Rust](https://tikv.org/blog/implement-raft-in-rust/)

- - -
- [ ] Write a loop that adds some stuff to raft
  - [ ] Somehow check that its in there

- [ ] Add a follower to raft
  - [x] CLI arg to get address
  - [x] Makes contact via RPC
  - [ ] Add a change entry
    - [ ] Based off of [raft-rs/main.rs at master · tikv/raft-rs · GitHub](https://github.com/tikv/raft-rs/blob/master/examples/five_mem_node/main.rs#L423)
  - [ ] Add message passing between the two via RPC
- [ ] Switch storage to file-based storage
  - [ ] Take an argument from the cli where to store the file
  - [ ] Change out MemNode
* Links
  * [The TiKV blog | Implement Raft in Rust](https://tikv.org/blog/implement-raft-in-rust/)
  * [fleetfs/storage_node.rs at master · fleetfs/fleetfs · GitHub](https://github.com/fleetfs/fleetfs/blob/master/src/storage/storage_node.rs#L100)
  * [GitHub - tikv/raft-rs: Raft distributed consensus algorithm implemented in Rust.](https://github.com/tikv/raft-rs/)

- [ ] Raft-SQL integration
  - [ ] This instead of tikv directly?
    - [ ] [GitHub - ritelabs/riteraft: RiteRaft - A raft framework, for regular people](https://github.com/ritelabs/riteraft)
  - [ ] [GitHub - rusqlite/rusqlite: Ergonomic bindings to SQLite for Rust](https://github.com/rusqlite/rusqlite)
  - [ ] [yuyang0/rrqlite](https://github.com/yuyang0/rrqlite)
    - [ ] Using https://github.com/datafuselabs/openraft

- [ ] Raft completeness
  - [ ] Snapshotting/state restore
  - [ ] Good logging story
  - [ ] Customize the raft config with file or arg inputs
  - [ ] Copy [chiselstrike/chiselstore: SQLite + Little Raft = 🚀](https://github.com/chiselstrike/chiselstore/)?

- [ ] Object storage in raft
  - [ ] CLI can submit a “job” object
    - [ ] Just a dummy object for now
  - [ ] Saves to raft
  - [ ] Queryable from any server node

- [ ] Connecting client nodes
  - [ ] Basics
    - [ ] Connection + Config
      - [ ] Issue token from server
        - [x] Command
        - [ ] Returns random JWT
        - [ ] JWT includes address
        - [ ] JWT includes auth type (OTT, Token, node identity)
        - [ ] Is signed by some secret key
        - [ ] Is printed to the console
        - [ ] Has a flag to be written to a file
      - [ ] Client accepts token and parses it
        - [ ] Command
        - [ ] Grab address
      - [ ] Client reaches out to address in there
        - [ ] GRPC call
          - [ ] Hello world
          - [ ] Print JWT
      - [ ] Server validates JWT signature
      - [ ] Server validates token is still valid
        - [ ] Optionally makes it invalid
      - [ ] Fingerprint data is sent to the server
    2. Resource ledger
    3. Heartbeating
       1. Run heartbeat background job?
       2. Mark unhealthy/healthy accordingly
          1. Be able to watch this change via the CLI
  2. Nice to haves
     1. Re-config
     2. Client restarts triggered from the servers
     3. Automatic configuration with same file
     4. Fingerprinting
     5. TLS
        1. Autoconfigured
        2. Auto-rotated??
     6. Dynamic Attrs
     7. Services??

- [ ] Running workloads
  1. Pass in a job file
  2. Runs a binary
- [ ] Running workloads II
  1. Exec task kind
     1. Copy Seth’s exec v2/pledge task kind implementation
  2. Podman task kind?
     1. Or just a docker-based task kind?
     2. Or something that can do both somehow?
     3. MUST work on a Mac easily (and on linux)
        1. Think about Kamal
        2. Think about local dev
  3. Libvirt task kind
     1. Use this instead? - [Run Cloud Virtual Machines Securely and Efficiently](https://www.cloudhypervisor.org/)?

- [ ] Scheduling
  1. Multiple scheduler algos

- [ ] Pluginification
  - [ ] Commands
  - [ ] API Routes (or grpc equiv)
  - [ ] Schedulers
  - [x] Fingerprinters
  - [ ] Jobspec parsing
  - [ ] Alloc preparation on node
  - [ ] Task Group prep on node
  - [ ] Job Submit Hooks
    - [ ] Job Mutation, Gating, Side Effect
  - [ ] Singleton services on the server?
    - [ ] IE an autoscaler service that’s just baked in
  - [ ] The UI? Pages within the UI?
    - [ ] Pages might be cool if it’s wasm, cause then you can just drop Rust in there
  - [ ] Storage
    - [ ] The DB in memory (just have it use built in sqlite initially?)
    - [ ] DB with something that streams the DB to some backup
      - [ ] And has a replica ready to go
    - [ ] The long term storage (raft optional)

## Things to be Done

#### Control Nodes (Server Nodes)
* Distributed State
  * Raft (probably)
  * Pluginable!
  * Reading state from file(s)
* Queries to state
  * SQLite
  * Pluginable!
* API
  * OpenAPI
  * Plugin???
* Keeping track of every node
  * Some heartbeating mechanism
* Evaluations queue(s)
  * When do we re-run checks for things?
* Scheduler
  * Ledger of all the nodes and stuff about them
  * Algorithm for placing
    * Plugin!
* Reading jobfiles
  * Plugin (multiple languages)
  * Plugin (custom resources under new keys)
  * Plugin (task kinds… are really just custom resources…???)
* Running & monitoring jobs
  * Plugin?
    * For different kinds
* Deployments
  * Plugin?
    * For different kinds
* Policies
  * Plugin?

#### Worker Nodes (Client nodes)
* Heartbeating
* Fingerprinting & Attributes
* Running tasks
  * Pluginable!
  * Task Ordering
    * Pluginable!
* Artifacts
  * Pluginable!
  * * With cache!
#### UI
* Automatic grpc UI to start!
* grpcui -proto proto/legion.proto -plaintext localhost:4747
* Plugginable!
  * Whole thing
  * Routes too
* Tools
  * Axum + HTMX?
  * Dioxus?
  * Tauri? - Prob not

## Plugins
* Schedulers
* Commands & API Endpoints
* Routes (for http API)
* Fingerprinters
* Networking fingerprinters (for different configs for Service Disco)
* CNI (or custom networking plugin that can do CNI)
* Allocation Setup Plugins
* And Task Group Setup plugins
* Configuration language (eventually all goes to JSON?)
* Custom Resources

* Things like Autoscaler should run as part of the core binary (and be pluginable), but then also be able to run outside as their own things. This would allow for simple Day 1 but performant Day N.
  * Also maybe ingress proxies?

* Legion should be able to be a librarh to other things. CLI shouldn’t be only interface ever.

## Differentiators
* Highly extensible
* Sqlite???
* No Raft?!?!
  * Litestream?
    * Both S3 + NFS?
  * At least initially this sounds pretty slick
  * You can have downtime on the leader and its NBD too…
  * This would make development + running tremendously simpler (and cheaper!)
  * Maybe mvsqlite
    * [A distributed SQLite benchmark: How mvSQLite scales](https://su3.io/posts/mvsqlite-bench-20220930)
  * Maybe rqlite? (Raft)
* GRPC
* OTEL
  * [open-telemetry/opentelemetry-rust: OpenTelemetry API and SDK for Rust](https://github.com/open-telemetry/opentelemetry-rust)
* Config lang?
  * jsonnet*
* Task kind implementation split into parts (isolations vs runner vs artifact)?
* networking using netavark (+ CNI)?
* Networking over unix domain sockets legion overlay network
* Apps that can use the DB but then get split out
  * Autoscaler
  * GitOps App
  * Overlay network controller?
  * Logging?
  * Buildpack runner
  * Cert manager?
* Ease
  * First experience should be super easy for small apps
    * Can push docker images directly into the worker node artifact cache to avoid setting up docker hub or whatnot?
      * Or into servers and they sent to the worker nodes??
    * Perhaps a proxy like Kamal has?
  * Automatic “build & push” step for Docker?
    * legion prep? Legion build?
    * Plus a “context” file like npm or James’ Nomad file?
* Root privs
  * Processes for networking + volumes as root but not main?
* Cert Manager built in for TLS stuff, Public CA routes?
* Node Configuration Jobs
  * Like a sysbatch but runs on join too, and adds dynamic node metadata
* Pluggable Node Statuses for degradation
  * Disk Pressure
  * Network Pressure
  * ETC ETC
* Allocations are a default resource type
* Node management “jobs”
  * CNI?
  * Volumes?
  * Artifacts
  * Ansible type stuff?
* Use virtual fs for the alloc directory!
  * From Tim in meeting
  * “If Nomad v2 existed this is what we’d do”
* Crazy idea -
  * Add in-code task/job definitions like Prefect ([Welcome to Prefect](https://docs.prefect.io/latest/))
  * Monetize with artifact store?
* Eval hooks?
  * If certain things happen on, you can define something to react from within legion?!
  * Can break off into its own job (this is like autoscaler maybe where its a plugin?)
* Port Forwarding
* Pre-fetching artifacts in deploy process
  * Artifact cache generally?
  * This should be built in for the scaling cases
  * Multiple layers of pre-fetching and tagging nodes as being pre-warmed

## Names
* Lots of little things
  * Flock
  * Legion
  * Skitter
  * Khepri
  * Horde
* Hydra
* Runner
* Runr

## Libraries for Later
* Axum web server [GitHub - tokio-rs/axum: Ergonomic and modular web framework built with Tokio, Tower, and Hyper](https://github.com/tokio-rs/axum)
* Utopia for OpenAPI spec if needed one day
* Alt config langs
  * * [GitHub - tweag/nickel: Better configuration for less](https://github.com/tweag/nickel)
  * Dhall?
    * No: too confusing
  * json2 or whatever its called?

#### Random

Steal from [Aurae Runtime](https://aurae.io/)
* Typescript job specs?!?!
* …better than yaml…
* grpc API for integrations?
  * Maybe doesn’t matter?!?

## Questions
* Can a “framworks” thing like Mesos has exist?
* Can you pre-allocate space to make scheduling decisions faster for incoming stuff?

* Should this be using a Fly.io-like scheduler system instead of the eval loop?
  * Does this even replace the eval loop?
  * Every client checks for what jobs need running
    * Submits a bid
    * After N seconds, scheduler gives it to the best option and tells it it got it
    * Each one will pre-warm ASAP but block on actual starting
    * This is dope for speed at scale
    * How would it work in the case of trying to optimize? (just set a longer time to evaluate and you’re good?)


### Various Links

[Introduction - Rust By Example](https://doc.rust-lang.org/stable/rust-by-example/)
[Introduction - Rust Design Patterns](https://rust-unofficial.github.io/patterns/#design-patterns)
https://docs.google.com/document/d/1kQidzAlbqapu-WZTuw4Djik0uTqMZYyiMXTM9F21Dz4/edit

https://www.youtube.com/watch?v=70_9IIsQfjs&list=PLai5B987bZ9CoVR-QEIN9foz4QCJ0H2Y8&index=21

[A half-hour to learn Rust](https://fasterthanli.me/articles/a-half-hour-to-learn-rust)

[SQLite Pragma Cheatsheet for Performance and Consistency](https://cj.rs/blog/sqlite-pragma-cheatsheet-for-performance-and-consistency/)

Orchestrator written in Rust

https://github.com/tikv/raft-rs/blob/master/examples/five_mem_node/main.rs#L199

https://news.ycombinator.com/item?id=39500397 - fly asking thing