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
* Plugginable!
  * Whole thing can be swapped out
  * Routes too
* Tools
  * Dioxus?
  * TanStack with Solid?

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

* grpc API for integrations?
  * Maybe doesn’t matter?!?

* Crazy idea -
  * Add in-code task/job definitions like Prefect ([Welcome to Prefect](https://docs.prefect.io/latest/))
  * Monetize with artifact store?

## Questions
* Can a “frameworks” thing like Mesos has exist?
* Can you pre-allocate space to make scheduling decisions faster for incoming stuff?


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