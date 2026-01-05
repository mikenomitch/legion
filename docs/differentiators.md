## Differentiators

### Most Important

* Highly extensible, many customizable plugin types:
  * Schedulers
  * Resources
  * Commands & API Endpoints
  * Fingerprinters
    * Networking fingerprinters (for different configs for Service Disco)
  * CNI (or custom networking plugin that can do CNI)
  * Allocation Setup Plugins (IE - download image or snapshot or cache data/files)
  * Artifacts (docker images, VM snapshots, etc)
    * Type, auth, how to get them, how to cache them
  * Task Group Setup plugins
  * Configuration language??
  * Pluggable Node Statuses for degradation (Disk Pressure, Network Pressure, ect)
  * Eval hooks?
    * If certain things happen on, you can define something to react from within legion?
    * Can break off into its own job (this is like autoscaler maybe where its a plugin?)

* All in one (omakase) experience
  * Autoscaler
  * Ingress Proxy

* Ease
  * First experience should be super easy for small apps
    * Can push docker images directly into the worker node artifact cache to avoid setting up docker hub or whatnot?
      * Or into servers and they sent to the worker nodes??
    * Perhaps a proxy like Kamal has?
  * Automatic “build & push” step for Docker?
    * legion prep? Legion build?
    * Plus a “context” file like npm or James’ Nomad file?

* Performance and Scale
  * Scales better than K8s (or even Nomad)

### Also Nice

* Nicer Config language (jsonnet)
* Sqlite interface for state
  * Makes for nicer nicer o11y and devex

* Raft Optional
  * Litestream?
    * Both S3 + NFS?
  * At least initially this sounds pretty slick
  * You can have downtime on the leader and its NBD too…
  * This would make development + running tremendously simpler (and cheaper!)
  * Maybe mvsqlite?
    * [A distributed SQLite benchmark: How mvSQLite scales](https://su3.io/posts/mvsqlite-bench-20220930)
  * Maybe rqlite? (Raft)

* GRPC
* OTEL built in
* Legion can be called as a library, not just CLI

* Apps that can use the DB but then get split out into jobs
  * Autoscaler
  * Ingress Proxy
  * GitOps Helper App
  * Overlay network controller?
  * Buildpack runner?
  * Logging?
  * Cert manager?
  * (Things like Autoscaler should run as part of the core binary (and be pluginable), but then also be able to run outside as their own things. This would allow for simple Day 1 but performant Day N)

* Cert Manager built in for TLS stuff, Public CA routes?
* Root privs
  * Processes for networking + volumes as root but not main?
* Node Configuration Jobs
  * Like a sysbatch but runs on join too, and adds dynamic node metadata
  * CNI, Volumes,Artifacts, Ansible type stuff
* Port Forwarding
* Pre-fetching artifacts in deploy process
  * Artifact cache generally?
  * This should be built in for the scaling cases
  * Multiple layers of pre-fetching and tagging nodes as being pre-warmed


### Maybe (on the table)

* Task kind implementation split into parts (isolations vs runner vs artifact)?
* Networking using netavark (+ CNI)?
* Networking over unix domain sockets legion overlay network

* Allocations are a default resource type
* Node management “jobs”
  * CNI?
  * Volumes?
  * Artifacts
  * Ansible type stuff?
* Use virtual fs for the alloc directory!
  * From Tim in meeting
  * “If Nomad v2 existed this is what we’d do”

* Should this be using a Fly.io-like scheduler system possibly instead of the eval loop? (depending on which scheduler you're running)
  * Does this even replace the eval loop?
  * Every client checks for what jobs need running
    * Submits a bid
    * After N seconds, scheduler gives it to the best option and tells it it got it
    * Each one will pre-warm ASAP but block on actual starting
    * This is dope for speed at scale
    * How would it work in the case of trying to optimize? (just set a longer time to evaluate and you’re good?)