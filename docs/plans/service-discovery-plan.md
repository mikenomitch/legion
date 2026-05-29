# Plan: Service Discovery

> **Related:** Service discovery is the connective tissue between several existing
> primitives. It *consumes* health from the existing [`checks`](../jobs-tasks-and-scheduling.md)
> model, reuses the [KV reactive-update](../kv-store.md) reconciliation path for
> endpoint-change actions, borrows the [Stateful Workloads & Instance Identity](stateful-workloads-and-instance-identity-plan.md)
> primitive for stable per-instance DNS, and derives mesh authorization identity from
> [Workload Identity via OIDC](workload-identity-oidc-plan.md). Read those for the
> primitives this plan builds *on top of*; this doc owns *how a workload names,
> finds, reaches, and authorizes another workload*.
>
> **Companions:** the [config cookbook, observability & mesh extensions](service-discovery-mesh-and-config.md)
> show how to configure all of this and how the mesh/observability layers attach; the
> [self-critique & revisions](service-discovery-critique.md) red-teams this plan against
> real-world failure modes. Revisions marked **[R]** below were folded in from that review.

## Problem Statement

Legion can place tasks but a task has no first-class way to **find and reach another
task**. Today an allocation ([src/structs/ledger.rs:12](../../src/structs/ledger.rs))
records `node_id`/`job_id`/`task_config_id` but **no port, no address, and no service
name**. There is no `network` block, no `service` block, and `Node`
([src/structs/node.rs:13](../../src/structs/node.rs)) exposes no routable address
outside of opaque `fingerprint_attrs`. The TASK-LIST states the gap exactly
([docs/TASK-LIST.md:86](../TASK-LIST.md)):

> - [ ] I can declare a named service on a task
> - [ ] I can declare health checks on that task  *(✅ `checks` already exist)*
> - [ ] Can query address and port from other services
> - [ ] I can declare which other jobs have access to my service

So a user can run `web` and `db` but `web` cannot discover `db`'s address, cannot be
told only-healthy `db` instances, and cannot restrict who connects to it. This plan
delivers all four, plus the data path that makes the discovered address actually
reachable — and it must do so **without bolting on a second system** (Consul) and
**without hard-depending on Linux-only kernel features** so it works on macOS too.

---

## Design Tenets (what we learned from prior art)

These tenets are derived from a survey of K8s, Nomad, Consul, Envoy/xDS, Linkerd,
Cilium, SmartStack, Eureka, and fly.io. Each tenet names the pain it avoids.

1. **One tier, no second system.** Nomad's split between *native* discovery and
   *Consul* discovery (different check kinds, different mesh story, a migration cliff)
   is a documented source of confusion. Legion's manager **is** the catalog. State
   lives in the existing SQLite store; no Raft-on-the-side, no gossip cluster, no
   Envoy control plane to operate. Make the built-in rich enough that no one reaches
   for a second system.
2. **The catalog is a *projection*, not a new source of truth.** A service entry is a
   materialized view over `(allocation, assigned port, check health)` tuples Legion
   already owns. Registration is not an app/agent action that can drift — it is
   derived the instant an allocation passes its checks and torn down when it stops.
   This sidesteps the stale-catalog failure mode (dead nodes lingering in Consul, znode
   leaks in SmartStack/ZooKeeper).
3. **DNS-native, fly.io-style, with no `ndots` tax.** Every node runs a tiny built-in
   resolver at a fixed address (like fly's `fdaa::3`). Discovery is plain DNS so it is
   language-agnostic with zero SDK — but we serve a flat, fully-qualified `.legion`
   zone and ship a resolver config with `ndots:1`, explicitly avoiding the K8s
   `ndots:5` latency trap (every external lookup paying 4 failed search-domain probes).
4. **Push-fresh, never poll-stale.** K8s DNS staleness comes from TTL polling racing
   fast endpoint churn. The manager **pushes** endpoint changes to node-local resolvers
   over the existing worker stream, sharded per-service (the EndpointSlice lesson:
   bound each update so a 5,000-endpoint service doesn't reship a megablob on every
   change). The node resolver is authoritative and always fresh; TTL is a backstop, not
   the mechanism. This is also a built-in NodeLocal-DNSCache: no central DNS hop on the
   hot path, no kube-proxy iptables rule explosion.
5. **L4 by default, L7 on demand, sidecar *never* mandatory.** Borrowing from Istio
   *ambient* and Cilium: the default data path is a **per-node** userspace forwarder
   (one per worker, à la ztunnel — *not* one sidecar per task). It does L4 + mTLS. L7
   (HTTP routing, retries, canary, per-request authz) is an **opt-in** waypoint-style
   proxy plugin, only for services that ask for it. This avoids the per-pod Envoy cost
   that drove the whole industry toward sidecarless.
6. **Identity, not IP, is the unit of authz.** "Which jobs can access my service" is
   answered by *intentions* keyed on workload **identity** (SPIFFE-style
   `region:job:group:task`), reusing the [Workload Identity / OIDC](workload-identity-oidc-plan.md)
   `sub`. Policy survives reschedules and IP churn — the Cilium/SPIFFE lesson.
7. **The data path is a plugin; the catalog is not.** *Service identity, the catalog,
   DNS, and intentions are platform-independent.* Only the mechanism that assigns an
   address and forwards a packet differs per platform/network. That mechanism is a
   **datapath plugin** (our modern CNI). This is the single design decision that makes
   macOS support tractable (see §8).
8. **[R] AP read, CP write — discovery survives a manager outage.** The catalog *write*
   path is CP (it projects Raft-committed scheduler truth — we *want* consistency when
   recording an endpoint). The discovery *read* path is **AP**: every node's resolver and
   forwarder serve their **last-known-good** endpoint set and keep resolving through a
   manager/Raft partition. New registrations may stall during a partition; existing
   discovery must not. This is the Eureka self-preservation / Fly local-DB / Tailscale
   pushed-map lesson — the whole survey agrees discovery wants A over C. Endpoint
   presence is further tied to a **worker heartbeat lease** (ZK-ephemeral / etcd-lease
   model): a dead worker's endpoints auto-expire with its lease, so there is no zombie
   reaper and no stale entry. And failure detection is **self-preserving**: when a large
   fraction of a node's or zone's endpoints fail health *at once*, suspect the
   network/observer, not mass death — hold last-good and alarm rather than mass-evict
   (the Lifeguard "a degraded detector distrusts its own judgment" lesson).

---

## Goals

- **Declare a service in one line.** `service: true` on a task with a port is enough to
  get a DNS name, env vars, only-healthy endpoints, and L4 reachability. Progressive
  disclosure for everything beyond.
- **Query address + port three ways**, each strictly simpler than the last to adopt:
  env vars (static, zero-dependency), DNS (dynamic, language-agnostic), and the
  existing template engine (`{{ service "db" }}`, config-file rendering, change-reactive).
- **Only-healthy contract.** A service resolves only to allocations currently passing
  their `health`/`readiness` checks; boot checks gate first registration.
- **Intentions / access control.** A service declares which identities may connect;
  default-deny is opt-in per cluster.
- **Works out of the box on Linux and (mostly) macOS** from a single binary, no Consul,
  no VM *required* for the common case.
- **A modern, declarative datapath plugin ABI** — with a **CNI compatibility shim** so
  the existing CNI ecosystem still works ("maybe CNI, maybe something more modern" → both).
- **Reuse, don't reinvent:** checks, KV reconciliation, the eval loop, the cdylib
  plugin runtime, instance identity, and workload identity are all existing machinery
  this plan extends.

## Non-Goals

- **A full L7 mesh / xDS control plane.** We define the L4 default and the *hook* for an
  L7 waypoint plugin; a complete Envoy-grade L7 feature set (outlier detection, complex
  traffic mirroring) is follow-up.
- **Multi-cluster / cross-region federation** (the MCS-API analog). Services are
  region-scoped in v1; a `ServiceExport`-style federation is a later plan.
- **A public ingress / Gateway implementation.** The built-in **ingress proxy** is its
  own effort (differentiators list it separately); this plan defines the *internal*
  east-west discovery it will consume, and sketches the Gateway-API-style role split it
  should adopt, but does not build the ingress.
- **Replacing `checks`.** We consume the existing check model; we do not redesign it
  (we only add a `grpc` kind and a service-scoped aggregate health).
- **Network isolation on macOS.** On Mac we deliberately fall back to host networking +
  userspace forwarding (see §8); namespace-grade isolation on Mac is out of scope.

---

## Background: What Exists Today

Grounding the design in current code so we extend rather than reinvent.

| Concern | Where | Notes |
| --- | --- | --- |
| Allocation record | [src/structs/ledger.rs:12](../../src/structs/ledger.rs) | `id`, `node_id`, `job_id`, `task_config_id`. **No port, no address, no service.** |
| Task spec | `TaskConfig` ([src/structs/job.rs:815](../../src/structs/job.rs)) | Where `network`, `service`, `allow` blocks attach. No networking fields today. |
| Health checks | `TaskCheck` / `TaskCheckKind` ([src/structs/job.rs:475](../../src/structs/job.rs), [:359](../../src/structs/job.rs)) | `http`/`tcp`/`exec`, plus `boot`/`readiness`/`health` types. The feed for only-healthy. Add `grpc`. |
| Node record | [src/structs/node.rs:13](../../src/structs/node.rs) | `fingerprint_attrs`/`dynamic_attrs` (JSON). A **networking fingerprinter** records routable addrs + available datapaths here. |
| Template engine | `KvTemplateSource` ([src/structs/job.rs:874](../../src/structs/job.rs)), `{{ kv "…" }}` | Already renders env/files from KV. Add `service`/`endpoints` template funcs. |
| KV → reconcile | [docs/kv-store.md](../kv-store.md) `on_change` actions | `none`/`restart`/`rollout`/`fail` vocabulary reused for endpoint-change reactions. |
| Eval loop | [docs/scheduler-and-eval-loop.md](../scheduler-and-eval-loop.md) | Eval kinds + dedup-on-claim; a new endpoint-sync trigger rides this. |
| Scheduler | `src/manager/scheduler.rs` | Port assignment happens at placement, alongside resource fit. |
| Plugin runtime | [src/plugin_runtime.rs](../../src/plugin_runtime.rs), [docs/plugin-system.md](../plugin-system.md) | In-process cdylib plugins (`get` symbol, `plugin.json`, `legionApiVersion`). The datapath plugin is a **new plugin kind** here. |
| Workload identity | [workload-identity-oidc-plan.md](workload-identity-oidc-plan.md) | SPIFFE-style `sub` — the identity intentions are keyed on. |
| Instance identity | [stateful-workloads-and-instance-identity-plan.md](stateful-workloads-and-instance-identity-plan.md) | Stable `instance_id`/slot → stable *per-instance* DNS (headless analog). |

There is **no** network, port, service, DNS, or intention concept in the codebase today
— this is greenfield, but it slots cleanly onto the primitives above.

---

## Proposed Design

### 1) Ports & the `network` block

A new optional `network` block on `TaskConfig` declares ports. Ports are the foundation:
a service advertises a port, env vars expose it, DNS SRV records carry it.

```jsonnet
network: {
  mode: 'auto',              // 'auto' | 'host' | 'bridge' | 'cni/<name>' | plugin name
  ports: [
    { name: 'http', to: 8080 },             // dynamic host port -> container 8080
    { name: 'metrics', static: 9090 },       // fixed host port
  ],
}
```

- **Dynamic ports** are assigned by the scheduler from a configurable range (default
  `20000–32000`, mirroring Nomad) at placement time and recorded on the allocation.
- **`mode: 'auto'`** asks the node's networking fingerprint which datapath is best:
  `bridge` on Linux with the bridge datapath present, `host` on macOS/bare nodes. The
  spec author writes `auto` and gets the right thing per platform (see §8).

Assigned ports/addresses are written back onto the `Allocation` (new `network` field,
§Persistence) so they are queryable and injectable.

### 2) Env-var discovery (the zero-dependency path)

At task launch the worker injects address vars via the existing env path (the same
pipeline `exec`/`container` already use, and that the instance-identity plan extends).
This mirrors Nomad's `NOMAD_*` so the pattern is familiar:

| Variable | Meaning |
| --- | --- |
| `LEGION_PORT_<label>` | Assigned host port for the named port. |
| `LEGION_ADDR_<label>` | `ip:port` for the named port. |
| `LEGION_IP_<label>` | Host/interface IP serving the port. |
| `LEGION_PORT_TO_<label>` | In-container target port (bridge mode). |

A service can read `LEGION_ADDR_http` at startup with no SDK and no resolver — the
"it just works for one service" path. Static-at-launch is its weakness; DNS/templates
cover dynamic.

### 3) The `service` block (registration)

A task advertises a service by adding a `service` block. The catalog entry is a
**projection** (Tenet 2): Legion registers an endpoint the moment the allocation's
checks pass and removes it when the allocation stops or goes unhealthy — the app does
nothing.

```jsonnet
// Minimal — the one-liner:
service: { port: 'http' }     // name defaults to the task name

// Fuller:
service: {
  name: 'api',                // DNS label; defaults to task name
  port: 'http',               // named port from network.ports
  tags: ['v2', 'public'],     // selectable in queries/DNS subdomains
  meta: { protocol: 'grpc' },
  // checks are the EXISTING `checks` block on the task — reused, not duplicated.
}
```

- **Only-healthy:** an endpoint enters the catalog only after `boot`+`readiness` checks
  pass and leaves it on the first `health` failure (the existing `on_unhealthy` policy
  still governs restart/reschedule independently).
- **[R] Deregister-before-stop (graceful drain).** On rollout/drain/stop the endpoint is
  **withdrawn first**, the resolver/forwarder is given a drain window to converge, and
  *only then* is the task sent SIGTERM. This closes the K8s "connection-refused during
  rolling update" race (endpoint-withdrawal vs. termination ordering). Because our
  forwarder is **per-node, not per-task**, it does not die with the app — structurally
  dodging the mesh "proxy died first" bug.
- **Service health aggregate:** a service exposes `healthy/total` endpoint counts for
  the UI and for L7 routing decisions.

> ★ Insight ─────────────────────────────────────
> Because the catalog is *derived* from allocation + check state Legion already commits
> to SQLite, there is **no separate registration RPC that can lie**. Consul's classic
> failure (an agent dies, its service lingers as healthy until a TTL/anti-entropy sweep)
> and SmartStack's (a stale ZooKeeper znode) are structurally impossible here: when the
> allocation row transitions, the projection transitions in the same transaction. The
> catalog cannot drift from the scheduler's truth because it *is* the scheduler's truth,
> re-shaped.
> ─────────────────────────────────────────────────

### 4) DNS-native discovery (the language-agnostic path)

Every worker runs a tiny built-in resolver (a userspace task in the core binary; later
splittable into its own job per the differentiators "Day 1 simple / Day N split"
pattern) bound to a **fixed address** the worker injects into each task's
`/etc/resolv.conf`. Default `127.0.1.53` (v4) and a ULA `fd00:1e:0::53` (v6),
configurable to dodge a host `systemd-resolved` on `127.0.0.53` (Open Question 1).

**Zone scheme — flat and fully-qualified to kill the `ndots` tax:**

| Name | Resolves to |
| --- | --- |
| `<service>.svc.legion` | A/AAAA of all *healthy* endpoints (client-side LB by record set) |
| `<region>.<service>.svc.legion` | healthy endpoints in a region (fly.io's `<region>.<app>.internal`) |
| `<tag>.<service>.svc.legion` | healthy endpoints carrying `<tag>` |
| `_<port>._tcp.<service>.svc.legion` | SRV records (port + target), for non-default ports |
| `<slot>.<service>.svc.legion` | a **specific instance** (slot-mode instance identity) — headless/StatefulSet analog |
| `vip.<service>.svc.legion` | the service's stable virtual address (L4 forwarder entry, §6) |
| `meta.<service>.svc.legion` (TXT) | **[R]** `service.meta` key/values — fly's `regions.`/DNS-SD TXT idiom, a metadata API over plain DNS |
| `_services.legion` (PTR) | **[R]** enumeration of all service types — the DNS-SD (RFC 6763) PTR convention |

We ship `resolv.conf` with `ndots:1` and a single `search svc.legion`, so
`db.svc.legion` (one trailing dot of intent) resolves in one query and external names
(`api.stripe.com`) never pay failed search-domain probes — the explicit fix for K8s's
worst-known DNS pathology. On macOS, where `/etc/resolv.conf` is not authoritative, the
worker installs a **scoped resolver at `/etc/resolver/legion`** instead. **[R]**

> ★ [R] Two DNS hazards the K8s ecosystem learned the hard way, designed out here:
> - **No UDP-over-NAT (the conntrack 5s race).** The resolver is reachable from a task
>   **without host masquerade** (bound on the bridge gateway / inside the task netns), so
>   task→resolver queries never traverse SNAT/DNAT and never hit the netfilter insert
>   race that gives K8s its infamous 5-second DNS tails. The resolver↔manager push path
>   is **TCP/stream**, not racy UDP.
> - **Bounded answers + TCP fallback (the headless-truncation trap).** A huge endpoint set
>   overflows a UDP DNS message (~88 A records); K8s silently returns a truncated set and
>   Go's pure resolver fails on the TC bit. We **cap + randomize** the UDP answer, set TC,
>   and serve the full set only over **TCP** or the `ResolveService` API. Big-fanout
>   consumers are steered to the **forwarder VIP** (real LB) rather than raw record sets —
>   which also fixes DNS's single-A-record load-imbalance.

**Freshness is push, not poll (Tenet 4):** the manager pushes per-service endpoint
deltas to each worker resolver over the existing worker gRPC stream
([proto/legion.proto:1783](../../proto/legion.proto) `service Worker`), sharded per
service. **[R]** Pushes are **per-resource validated and partially applied** — a single
malformed service entry is dropped and alarmed, never rejecting the whole node update
(the xDS NACK-all lesson). The resolver answers from that always-fresh local view, and
**keeps serving its last-known-good set through a manager partition** (Tenet 8); the DNS
TTL (a few seconds) is only a backstop for clients that cache. No central DNS server on
the hot path → no single bottleneck, no cross-node hop.

### 5) Template discovery (the change-reactive path)

Extend the existing template engine (`{{ kv "…" }}`, [src/structs/job.rs:874](../../src/structs/job.rs))
with service functions — direct Nomad/`consul-template` parity:

```jsonnet
files: [{
  path: '/etc/app/upstreams.conf',
  file: { template: |||
    {{ range endpoints "db" }}server {{ .Addr }}:{{ .Port }};
    {{ end }}
  ||| },
  // reuse the EXISTING on_change vocabulary from KV injections:
  service: { on_change: 'restart' },   // none | restart | rollout | signal
}]
```

When the endpoint set changes, Legion re-renders and applies the same
`on_change` action vocabulary the [KV reactive path](../kv-store.md) already implements
(`restart`/`rollout`/`fail`, plus `signal` from the WI plan). This unifies "config
changed" handling across KV, identity tokens, and service endpoints under one mechanism.

**[R] Two anti-stampede rules, learned from Nomad's `consul-template` pain:**
- **Coalesce + splay + cap.** Endpoint deltas are debounced and `on_change` actions fire
  after a randomized `splay`, with a cap on concurrent restarts — so a popular service
  changing does not restart all its consumers in one thundering herd.
- **Fail static, never fail the task.** If a template can't render (a discovery blip),
  Legion **keeps the last-good rendering** and marks the task degraded — it never kills a
  healthy task because discovery hiccuped (Nomad's "task fails on render failure" +
  recovery-retry-storm is a documented cascading-outage generator).

### 6) The data path: L4 default, L7 opt-in (Tenet 5)

Discovery returns an address; the *data path* makes it reachable. Two layers:

- **L4 (default, always on, one per node):** a userspace forwarder on each worker — the
  ztunnel/SmartStack-local-HAProxy model, but **one per node, not per task**. Clients
  connect to the service VIP (`vip.<service>.svc.legion`) or a node-local listener; the
  forwarder picks a healthy endpoint (from the same pushed, always-fresh set) and
  forwards. It originates/terminates **mTLS** using workload-identity certs. Being pure
  userspace, **it runs identically on Linux and macOS** — the crux of §8. On Linux it
  can be accelerated by the datapath plugin (eBPF socket-LB / IPVS) to skip the
  userspace hop, but correctness never depends on that — sidestepping kube-proxy's
  iptables O(n) rule blowup by never building an O(n) rule set in the first place.
- **L7 (opt-in per service):** a `connect.l7 {}` block attaches a **waypoint** proxy
  (an Envoy/pingora datapath plugin) for that service, enabling HTTP/gRPC routing,
  retries, timeouts, traffic-splitting (canary), and per-request authz. Deployed per
  service-identity (waypoint model), not per task — so L7 cost is paid only where asked.

```jsonnet
connect: {
  upstreams: [{ service: 'db', local_port: 6379 }],  // app dials localhost:6379
  l7: { protocol: 'http', retries: 2, timeout: '3s' }, // OPT-IN; omit for pure L4
}
```

With `upstreams`, the app dials `localhost:<local_port>` and the node forwarder routes
to healthy `db` — the transparent-local-port ergonomics of SmartStack/Connect, with no
sidecar injected into the task.

### 7) Intentions (who can access my service) (Tenet 6)

A service declares allowed callers by **identity**, not IP. Identity is the
SPIFFE-style `sub` from the [WI plan](workload-identity-oidc-plan.md)
(`region:job:group:task`). This satisfies the TASK-LIST's "declare which other jobs
have access."

```jsonnet
service: {
  port: 'http',
  allow: [
    { job: 'frontend' },                 // any task in job 'frontend'
    { job: 'admin', task: 'cli' },        // a specific task
    { l7: { path: '/public/*', methods: ['GET'] } }, // L7 intention (needs connect.l7)
  ],
}
```

- **L4 intentions** gate *connections*; the forwarder checks the caller's mTLS identity
  against `allow` before proxying — identity proven by cert, not asserted by IP.
- **L7 intentions** gate *requests* (path/method/header) and require the L7 waypoint.
- **Default policy** (allow-all vs default-deny) is a cluster setting; default-deny is
  the zero-trust posture, opt-in so the first-run experience stays frictionless.

### 8) Multi-platform: the datapath plugin is the whole trick (Tenet 7)

Network namespaces, veth, bridges, and iptables are **Linux-only**. Nomad's honest
answer is "bridge/CNI on Linux, host-only everywhere else"; Docker Desktop's is "run a
Linux VM." We do better by splitting concerns:

- **Platform-independent (works fully on Mac, no VM):** the catalog, DNS resolver,
  env-var injection, template rendering, intentions, and the **userspace L4 forwarder**.
  These are all pure userspace. So the *90% case* — "name a service, find it, connect to
  it, restrict who connects" — works **identically on macOS and Linux**.
- **Platform-specific (the datapath plugin):** assigning an isolated IP and wiring an
  interface. This is the *only* part that needs Linux kernel features, and it is behind
  the plugin boundary.

Concretely, `network.mode: 'auto'` resolves via the node's networking fingerprint:

| Platform | Datapath plugin | Isolation | Discovery |
| --- | --- | --- | --- |
| Linux (bridge/CNI present) | `bridge` (built-in) or `cni/<name>` | full netns isolation, per-alloc IP | full |
| Linux (eBPF datapath plugin) | `ebpf` | netns + kernel socket-LB | full, accelerated |
| macOS / bare node | `host` (built-in) | none — host net + dynamic port map | **full** (DNS + userspace forwarder) |

The Mac story is therefore "host networking + dynamic ports + the userspace forwarder +
DNS" — which delivers real service discovery, just without namespace isolation. That is
strictly more than Nomad gives on Mac, with no VM. (A future `vmnet`/userspace-overlay
datapath plugin could add Mac isolation; out of scope here.)

### 9) The datapath plugin ABI — a modern CNI (Tenet 7)

CNI's critiques: it fork/exec's binaries passing JSON (imperative, hard to debug), its
chaining is underspecified, it is L3/L4-only and node-local with no service/identity
concept, and it makes cluster-centric assumptions that misfit single-node tools (why
Podman left CNI for netavark). Legion already has a better substrate: **in-process
cdylib plugins** ([docs/plugin-system.md](../plugin-system.md)) — fast, no fork/exec, a
validated `plugin.json` manifest. We add a new plugin **kind: `datapath`** with a
*declarative, reconciled, idempotent* ABI (not CNI's one-shot ADD/DEL):

```
trait DatapathPlugin {
  fn fingerprint(&self, node) -> DatapathCapabilities;       // routable addrs, modes available
  fn reconcile_alloc(&self, alloc, desired: AllocNet) -> AllocNet; // idempotent; returns assigned ip/ports
  fn teardown_alloc(&self, alloc);                            // idempotent cleanup + GC of leaks
  fn reconcile_service(&self, service, endpoints: &[Endpoint]); // program L4 forwarder/eBPF; idempotent
  fn status(&self) -> DatapathStatus;                         // readiness to accept allocs
}
```

Differences from CNI, each fixing a named critique:
- **Declarative reconcile, not imperative ADD/DEL:** every call states desired end-state
  and is idempotent — re-runnable after a crash, no leaked half-state; includes explicit
  GC (CNI only recently bolted on `GC`).
- **One coherent plugin owns interface + IPAM + service datapath** (the Cilium "do it
  coherently" lesson) instead of CNI's fragile binary chain where ordering and
  `CNI_ARGS` ownership are perennial bugs.
- **Service- and identity-aware:** `reconcile_service` knows about services and healthy
  endpoints — CNI stops at "attach an interface" and forces kube-proxy + mesh on top.
- **In-process & fast:** no per-alloc process spawn.

…**and a `cni` built-in datapath plugin is the compatibility shim**: it implements the
ABI by invoking standard CNI plugins (ADD/DEL/CHECK against `/etc/cni/net.d/`), so the
entire CNI ecosystem (Calico, Cilium-CNI, the reference bridge plugin, `consul-cni`)
works unchanged. That literally answers "maybe CNI, maybe something more modern" with
**both**: a modern native ABI *and* a CNI bridge. The networking fingerprinter
(differentiators: "Networking fingerprinters for different configs for Service Disco")
populates `DatapathCapabilities` into `node.fingerprint_attrs` so the scheduler and
`mode: 'auto'` can choose.

### 10) Ingress role split (sketch, not built here)

When the built-in ingress proxy lands, it should adopt the **Gateway API** role split
(K8s's fix for Ingress' annotation soup): a cluster operator provisions a *gateway*
(listener/entry point) and app teams attach *routes* in their own job specs, with the
operator setting policy — no non-portable annotations. The internal discovery defined
here is exactly what those routes target. Flagged so the service model stays
forward-compatible with it.

---

## Job Spec API (consolidated)

Minimal — discoverable, reachable, in one block:

```jsonnet
{
  name: 'db', kind: 'container',
  network: { ports: [{ name: 'redis', static: 6379 }] },
  service: { port: 'redis' },
  checks: [{ type: 'health', kind: 'tcp', port: 6379, every: '10s' }],
}
```

A consumer dialing it, with access control and dynamic ports:

```jsonnet
{
  name: 'web', kind: 'container', count: 3,
  network: { mode: 'auto', ports: [{ name: 'http', to: 8080 }] },
  service: {
    name: 'web', port: 'http', tags: ['public'],
    allow: [{ job: 'gateway' }],            // only the gateway may connect
  },
  connect: { upstreams: [{ service: 'db', local_port: 6379 }] }, // dial localhost:6379
  checks: [{ type: 'readiness', kind: 'http', path: '/ready', port: 8080, every: '5s' }],
}
```

Per AGENTS.md, every new field is added to [configs/jobs/maximal.jsonnet](../../configs/jobs/maximal.jsonnet)
showing the full surface (network modes, multiple ports, tags/meta, L4 + L7 connect,
L4 + L7 intentions, template `endpoints` function).

---

## Proto, Struct, and Persistence Changes

1. **`src/structs/`**
   - new `network.rs`: `NetworkConfig`, `PortConfig`, `AllocNetwork` (assigned ip/ports),
     `DatapathCapabilities`.
   - new `service.rs`: `ServiceConfig`, `Endpoint`, `Intention`, `ConnectConfig`.
   - extend `TaskConfig` ([src/structs/job.rs:815](../../src/structs/job.rs)) with
     `network`, `service`, `connect`.
   - extend `Allocation` ([src/structs/ledger.rs:12](../../src/structs/ledger.rs)) with
     `network: Option<AllocNetwork>`.
   - add `Grpc` to `TaskCheckKind` ([src/structs/job.rs:359](../../src/structs/job.rs)).
2. **`proto/legion.proto`** — new `Manager` RPCs: `ListServices`, `GetService`,
   `ResolveService` (endpoints + ports, for the resolver/template/UI); extend the
   `Worker` stream ([proto/legion.proto:1783](../../proto/legion.proto)) with a
   per-service endpoint push message; carry assigned ports on alloc-related messages.
   Every method/field documented per AGENTS.md proto-comment rule. Regenerate via
   `build.rs`.
3. **Migration** ([src/storage/migrations.rs](../../src/storage/migrations.rs)) — add
   `network` column to allocations; `services` and `service_endpoints` views/tables
   (the projection); `intentions` table; a `port_allocations` index for dynamic-port
   bookkeeping.
4. **Scheduler** (`src/manager/scheduler.rs`) — dynamic-port assignment during placement
   (fit ports alongside resources; respect static-port conflicts as a hard filter);
   write `AllocNetwork` onto the allocation.
5. **Eval loop** ([docs/scheduler-and-eval-loop.md](../scheduler-and-eval-loop.md)) —
   on allocation health/lifecycle transition, recompute the affected service's endpoint
   set and enqueue a (dedup-on-claim) endpoint-push to workers running consumers /
   resolvers; reuse the template re-render + `on_change` path from KV.
6. **Worker** — networking fingerprinter populates `DatapathCapabilities`; datapath
   plugin invocation at alloc setup/teardown; node-local resolver + L4 forwarder
   lifecycle; env-var injection; resolv.conf management.
7. **Plugin runtime** ([src/plugin_runtime.rs](../../src/plugin_runtime.rs)) — register
   `datapath` plugin kind + manifest validation; ship built-ins `host`, `bridge`, `cni`.
8. **Docs** — new `docs/service-discovery.md` (user-facing), `docs/datapath-plugins.md`
   (ABI), update README doc list, plugin-system "Planned categories", and TASK-LIST.

---

## Implementation Phases

Each phase is independently shippable and leaves the tree green. **[R]** A hard line
separates **v1** (a complete product — discoverable + reachable by DNS/env/template, with
*no new data-plane component*) from **v2** (the opt-in mesh). The full TASK-LIST is
satisfied by v1 plus Phase 5's intentions; everything else is separately adopted. See the
[critique](service-discovery-critique.md) Part E for the rationale.

> **── v1: the actual product (no new data-plane component) ──**

### Phase 1 — Ports & env-var discovery
- `network` block, dynamic/static port assignment in the scheduler, `AllocNetwork` on
  the allocation + migration, `LEGION_*` env injection.
- **Exit:** a task declaring `network.ports` starts with `LEGION_ADDR_<label>` set and
  the port reachable in `host` mode on both Linux and macOS.

### Phase 2 — Service catalog (the projection)
- `service` block; catalog tables as a projection over alloc + check health; only-healthy
  registration + **deregister-before-stop** drain; **[R] liveness-as-lease** (endpoints
  tied to worker heartbeat) and **self-preservation** (no mass-evict on correlated
  failure); `ListServices`/`GetService`/`ResolveService` RPCs; UI surfacing.
- **Exit:** registering `db` shows exactly its healthy endpoints; killing a `db` alloc
  removes its endpoint within one health interval; a dead worker's endpoints vanish on
  lease expiry; a rolling update drains without connection-refused.

### Phase 3 — DNS + template discovery
- Node-local resolver (**[R] no-NAT path, TCP fallback, bounded answers, PTR/SRV/TXT**),
  `.svc.legion` zone, `ndots:1` resolv.conf (`/etc/resolver/legion` on macOS), push-based
  endpoint deltas over the worker stream (**per-resource validated, partial apply,
  last-known-good through partition**); `endpoints`/`service` template functions wired to
  the existing `on_change` path with **[R] coalesce/splay/cap + fail-static**.
- **Exit:** `web` resolves `db.svc.legion` to only-healthy endpoints; a config template
  re-renders + reloads on endpoint change without a thundering herd; a discovery blip
  does not kill consumers; external DNS pays no search-domain tax; lookups keep working
  during a manager partition.

> **── v2: the opt-in mesh (each independently justified) ──**

### Phase 4 — Datapath plugin ABI + bridge/CNI
- `datapath` plugin kind; built-in `host` (Phase 1 already), `bridge` (Linux netns +
  per-alloc IP), and `cni` compatibility shim; networking fingerprinter populates
  capabilities; `mode: 'auto'` selection.
- **Exit:** the same job runs with `mode: 'auto'` → bridge isolation on Linux, host on
  macOS, full discovery on both; a standard CNI bridge config works via the shim.

### Phase 5 — L4 forwarder + mTLS + L4 intentions
- Per-node userspace forwarder; service VIP + `connect.upstreams` local ports;
  mTLS via workload-identity certs (**[R] depends on the WI plan; jittered renewal to
  avoid rotation storms; a non-mTLS L4 mode for clusters pre-WI**); `allow` L4 intentions
  enforced at connect. **[R] Forwarder has explicit SLOs** (added-p99 budget, zero-drop
  reload via FD hand-off / `SO_REUSEPORT`); DNS-direct + client retry remains a
  first-class fallback so the forwarder is never the *only* path.
- **Exit:** `web` dials `localhost:6379` → reaches healthy `db` over mTLS; a caller not
  in `allow` is refused at connection with a clear diagnostic; a forwarder upgrade drops
  zero connections.

### Phase 6 — L7 waypoint (opt-in)
- `connect.l7` waypoint plugin (HTTP/gRPC routing, retries, timeouts, traffic split);
  L7 intentions (path/method); **[R] fail-open locality** (locality is a preference, never
  a partition — widen to region/global rather than serve zero/dead local endpoints).
- **Exit:** a canary split and a path-scoped intention work for an L7-enabled service;
  L4-only services pay no L7 cost; killing all local-zone endpoints fails over instead of
  black-holing.

### Phase 7 — WASM middleware → 7b native cdylib
- Sandboxed proxy-wasm-style middleware with non-optional fuel/memory/fail-policy limits;
  then operator-only native cdylib behind a cluster flag. See the
  [mesh companion](service-discovery-mesh-and-config.md) Part D.

### Phase 8 (later) — Prepared-query failover + multi-region
- **[R]** `<name>.query.legion` named queries with embedded RTT-aware failover policy
  (Consul prepared-query idiom); cross-region single-hop zone-ingress (Kuma idiom).

### Phase 9 (later) — `legion://` gRPC proxyless resolver
- **[R]** A gRPC name-resolver returning endpoints **+ LB/routing config** so gRPC apps
  skip the forwarder hop (proxyless path), per-resource (no NACK-all).

---

## Testing Plan

### Unit
- Port assignment: dynamic ports stay in range and never collide with static ports or
  each other on a node; static-port conflict is a hard placement rejection.
- Catalog projection: endpoint set is exactly the healthy allocations; boot/readiness
  gate entry; first `health` failure removes the endpoint.
- DNS: A/AAAA/SRV/region/tag/slot record generation; resolv.conf emits `ndots:1`;
  an external name resolves in a single upstream query (no search-domain probes).
- Intentions: `allow` matching by job / job+task / identity; default-deny vs allow-all.
- Spec validation: `service.port` must reference a declared `network.ports` label; `l7`
  intentions require `connect.l7`; `mode` values validated against fingerprint.
- Template funcs: `endpoints`/`service` render correctly; `on_change` maps to the
  existing action vocabulary.

### Integration
- End-to-end discovery: `web` resolves and connects to `db` via env var, via DNS, and
  via `connect.upstreams` local port — on **both** a Linux node and a macOS node.
- Only-healthy under churn: kill/restart `db` allocs; consumers never resolve a dead
  endpoint beyond one health interval; push delta beats TTL.
- Datapath parity: identical job, `mode: 'auto'` → bridge isolation on Linux, host on
  Mac, discovery green on both; CNI shim against the reference bridge plugin.
- mTLS + intentions: allowed identity connects; disallowed identity refused; identity
  survives a reschedule (new IP, same `sub`, still allowed).
- L7: canary traffic-split ratio within tolerance; path intention blocks a disallowed
  method.
- Reconcile idempotency: crash the worker mid-`reconcile_alloc`; on restart no leaked
  interfaces/ports (GC verified).
- **[R] AP read under partition:** sever the worker↔manager link; existing names still
  resolve to last-known-good endpoints; new registrations stall (not error existing).
- **[R] Liveness-as-lease:** kill a worker process without graceful deregister; its
  endpoints disappear on lease expiry with no manual reaping.
- **[R] Self-preservation:** fail a large fraction of a zone's checks simultaneously
  (simulated network blip); endpoints are held, not mass-evicted, and an alarm fires.
- **[R] Deregister-before-stop:** during a rolling update, a continuous client sees zero
  connection-refused (endpoint withdrawn + drained before SIGTERM).
- **[R] No-UDP-over-NAT + truncation:** task→resolver path crosses no SNAT/DNAT; a
  service with thousands of endpoints returns a capped UDP set with TC + working TCP
  fallback (verified against a Go pure-resolver client).
- **[R] Render anti-stampede:** a popular service's change does not restart all consumers
  at once (splay/cap honored); a forced render failure keeps last-good and does not kill
  the task.
- **[R] Partial apply:** a malformed single service in a push is dropped + alarmed; other
  services on that node keep resolving (no NACK-all).

### Regression
- Jobs with **no** `network`/`service` block behave exactly as today (no ports, no
  catalog entry, unaffected scheduling).

---

## Open Questions

1. **Resolver address & host DNS coexistence.** `127.0.1.53` vs a ULA vs a link-local
   address — how to guarantee no clash with host `systemd-resolved` (`127.0.0.53`) and
   how to inject into both `exec` (host resolv.conf / macOS `/etc/resolver/legion`) and
   `container` (per-container DNS) tasks uniformly? **[R]** The no-NAT requirement (bind
   on bridge gateway / in-netns) narrows the options — confirm a single scheme works
   across `host`/`bridge`/`cni` datapaths and both OSes.
2. **macOS host-mode port collisions.** Without netns every alloc shares the host port
   space; dynamic ports mostly handle it, but static-port jobs can collide. Reject at
   placement, or auto-remap with a warning?
3. **Service naming scope.** Names are flat per region today. Do we need namespaces/
   projects before multi-tenant clusters, or is `job` qualification (`<svc>.<job>.svc.legion`)
   enough? Affects DNS zone depth (and the `ndots` budget).
4. **L4 forwarder data path on Linux.** Userspace-always (simple, uniform with Mac) vs.
   eBPF/IPVS acceleration via the datapath plugin (faster, Linux-only) — when does the
   accelerated path kick in, and how do we keep semantics identical to userspace?
5. **Endpoint push fan-out (scoped subscription).** Per-service sharding bounds payload,
   but a 10k-consumer service still fans out widely. **[R] Leaning yes:** consumers
   subscribe only to services they declare in `connect.upstreams`/templates (scoped push),
   not every worker getting every service — confirm this is sufficient and how
   wildcard/DNS-only consumers (no declared upstream) are handled.
6. **Intention default & rollout.** Cluster-wide default-deny is the zero-trust ideal but
   breaks first-run ergonomics. Default allow-all with a one-flag flip, or per-namespace?
7. **Stateful/headless interplay.** Slot-mode per-instance DNS (`<slot>.<svc>.svc.legion`)
   depends on the instance-identity plan landing; confirm the dependency ordering and
   whether keyed-mode (sandbox) instances get DNS at all.
8. **Resolver/forwarder as core vs split job.** Start in-core (Day 1 simplicity) with a
   clean seam to split into their own jobs at scale (differentiators pattern) — what is
   that seam exactly?
9. **[R] Lease vs check health interplay.** Endpoint presence is gated by *both* the
   worker heartbeat lease (liveness) and check health (readiness). What is the precedence
   when they disagree (e.g. lease alive but checks failing during a node brown-out), and
   how does self-preservation's "correlated-failure" threshold get tuned per cluster?
10. **[R] Drain window length.** Deregister-before-stop needs a convergence window before
    SIGTERM; too short reintroduces the connection-refused race, too long slows rollouts.
    Fixed default, per-service `drain` knob, or derived from DNS TTL + push latency?

---

## Acceptance Criteria

- A task declares a service in **one line** (`service: { port: 'http' }`) and is
  discoverable by name with only-healthy endpoints.
- A consumer finds another service's address+port via **all three** paths (env var, DNS,
  template) and successfully connects, **on both Linux and macOS, with no Consul and no
  VM**.
- A service restricts callers by **identity** (`allow`), enforced at L4 over mTLS, and
  policy survives reschedules/IP churn.
- Discovery is **push-fresh**: a killed endpoint disappears within one health interval,
  and external DNS resolution pays no `ndots` search-domain penalty.
- The data path is **L4 by default with no per-task sidecar**; L7 is opt-in and costs
  nothing for services that don't ask for it.
- Networking is a **declarative datapath plugin** with built-in `host`/`bridge` and a
  **CNI compatibility shim**; `mode: 'auto'` picks the right datapath per platform.
- Jobs with no network/service block are **completely unaffected**.
