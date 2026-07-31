# Plan: Network Handlers — Ingress and Egress Middleware

> **Judgement calls:** [`calls/network-handlers-plan_calls.md`](../calls/network-handlers-plan_calls.md)

> **Status: PLANNED — no code written.** This plan introduces a new user-facing
> jobspec surface (`network.ingress` / `network.egress`), a new worker
> subsystem (the *chain*), a new manager RPC, a new security boundary, and a
> new sandbox capture mode. It composes with — and does not supersede —
> [service-discovery-final-plan.md](service-discovery-final-plan.md), whose
> `connect.upstreams` forwarder is the datapath this plan generalizes.
>
> It also **delivers three items other plans are blocked on**: "mTLS between
> forwarders" (service discovery §8 item 3), "ingress proxy / load balancer"
> (TODO § Networking), and the ingress dependency that
> [autoscaler-plan.md](autoscaler-plan.md) names as the blocker for
> wake-on-request scale-to-zero ("Needs the ingress proxy").
>
> **Deliberately deferred and named as such:** WASM handlers, `cdylib`
> handlers, HTTP/2 and HTTP/3, transparent egress capture for `container`
> (needs the datapath plugin), and a full L7 router. See §5 Non-goals.

---

## 1. The Problem

A sandbox is a microVM running somebody else's code — an agent, a customer's
build, an untrusted notebook. The operator running that fleet needs to answer
three questions about every byte that crosses its boundary, and today Legion
cannot answer any of them.

**Today, concretely:**

- A `sandbox` task has `network: { mode: 'none' }` (loopback only, the default)
  or `mode: 'tap'` — attach the guest to a **pre-created** host TAP device that
  Legion refuses to make, because it needs `CAP_NET_ADMIN` and a host
  addressing plan
  ([src/task_plugins/sandbox.rs:121](../../src/task_plugins/sandbox.rs),
  [docs/task-runtime-sandbox.md](../../docs/task-runtime-sandbox.md#networking)).
  There is no middle setting. Either the workload has no network, or it has
  *the operator's whole network* with nothing in between.
- `container` on Linux runs in the **host network namespace** — asserted as a
  test invariant at
  [src/task_plugins/oci_bundle.rs:669](../../src/task_plugins/oci_bundle.rs)
  ("no private network namespace: container uses host networking"). `exec` is
  a host process. So the same all-or-nothing holds everywhere.
- The one piece of datapath Legion owns —
  [`src/worker/forwarder.rs`](../../src/worker/forwarder.rs), the loopback
  forwarder behind `connect.upstreams` — splices sockets with
  `copy_bidirectional` and emits **no metrics at all**. It cannot deny a
  connection, cannot see a request, and cannot tell you who talked to whom.
- `service.allow` intentions are enforced by the manager *omitting* endpoints
  from a heartbeat feed. The public doc says so plainly: it "does not stop a
  task that already knows a backend's `address:port` … nothing on that wire is
  authenticated or encrypted"
  ([docs/service-discovery.md](../../docs/service-discovery.md)).
- There is **no ingress path**. A service's advertised endpoint is the task's
  own host port; consumers dial the workload directly. Nothing can sit in
  front of it.

**What an operator has to do instead, today.** Run the sandbox with
`mode: 'none'` and give it no network at all — which breaks any workload that
needs to `pip install` — or run it with `mode: 'tap'`, hand-build a bridge,
hand-write `nftables` rules on every worker, hand-roll a log pipeline off
`conntrack`, and accept that none of it is expressed in the jobspec, versioned
with the job, visible to `legion job plan`, or reproducible on a new node. The
policy lives in a Chef recipe; the job spec knows nothing about it.

**What this plan builds.** One primitive — an ordered **chain** of handlers
that Legion positions on a workload's ingress and egress paths — with a
declarative rules handler for the simple case, an out-of-process escape hatch
for the arbitrary case, and a sandbox capture mode where the chain is
*unbypassable by construction* because the guest has no network device at all.

---

## 2. Prior Art

| System | What it does | What we take | What we reject |
| --- | --- | --- | --- |
| **Envoy / Istio** — HTTP filter chain, `ext_authz`, `ext_proc` | A chain of filters per listener; external services get the request head over gRPC and return allow/deny/mutate | The **chain** shape, and the split between "Envoy does framing and TLS, your service makes decisions". `ext_proc` is exactly the right contract for an external handler | The sidecar-per-pod lifecycle (Legion's service-discovery plan already rejected it, §1, and Istio itself retreated from it with ambient mesh), and the CRD sprawl where a `DestinationRule` applied out of order 503s your traffic |
| **Cilium / eBPF** | Kernel-level L3–L7 policy, identity-based, no proxy hop for L4 | Identity-keyed policy (not IP-keyed) — dashboards and rules survive reschedules | Kernel dependence. Legion runs on macOS and on Linux without `CAP_NET_ADMIN`; an eBPF-only answer would work on neither |
| **Kubernetes NetworkPolicy** | Declarative allow-list, namespace/label selectors, default-deny opt-in | The **match/action allow-list** grammar and default-deny as an explicit posture | L3/L4 only — it cannot express "GET `/repos/**` on `api.github.com`", which is the actual sandbox requirement. And it is silently a no-op when no CNI implements it |
| **Consul intentions + Connect** | Service-to-service authz on cryptographic identity, sidecar proxies | Identity-anchored authorization, and `service.allow` as the existing policy input it should read | The sidecar again, and the second control plane |
| **AWS PrivateLink / egress firewalls, Cloudflare Gateway** | Central egress chokepoint with domain/SNI/URL policy and per-identity logs | The **egress-first** framing. Every one of the requested capabilities (per-user o11y, dynamic endpoint lockdown, filtering what leaves) is an egress capability | A separate appliance to run |
| **gVisor / Firecracker jailer patterns** | The sandbox has no host network; a host-side broker mediates | **The core idea of §7.3**: no NIC in the guest, all traffic over `vsock` to a host-side proxy. Unbypassable without any host firewall | — |
| **HAProxy PROXY protocol v2** | Prepends connection metadata (and arbitrary TLVs) to a proxied TCP stream | The **external handler metadata contract**. Every serious proxy already parses it, so a Legion handler needs no SDK | — |

**The synthesis.** Nobody gives you "run my code on the sandbox's network path"
without either a kernel dependency, a sidecar per workload, or a second control
plane. Legion can, because it already owns a userspace datapath on every node,
already builds the sandbox's root disk, and already has identity, health,
catalog, and heartbeat delivery. The chain is mostly *wiring things Legion
already has*, which is why §8 is short.

---

## 3. Jobs To Be Done

**Sandbox platform operator** — runs a fleet of per-user sandboxes.

- When a user's sandbox is running untrusted code, I want it to reach only the
  package registries and APIs I named, so a prompt injection cannot exfiltrate
  anything to an address I did not approve.
- When I discover a bad endpoint at 3am, I want to block it for every running
  sandbox in under a minute, so I do not have to restart thousands of live
  workspaces to ship a policy change.
- When finance asks who is calling the expensive API, I want per-user attribution
  of every outbound call, so I can bill it and cap it.
- When a sandbox sends data to an approved endpoint, I want to strip the things
  that must never leave, so an approved destination is not an approved payload.

**Job author / app developer**

- When I ship a service, I want to say "only these callers, over mTLS" in the
  same file as the rest of my job, so there is nothing to apply separately and
  nothing to apply in the wrong order.
- When my own proxy already does something clever, I want to drop it into the
  path without rewriting it as a plugin, so I keep the language and the tests I
  have.

**Security / platform admin**

- When I set an organization-wide rule, I want it enforced whether or not the
  job author opted in, so compliance does not depend on every team remembering.
- When someone claims traffic was blocked, I want a record naming the rule that
  blocked it, so I can tell a real denial from a broken app.

**On-call responder**

- When a service suddenly cannot reach its dependency, I want one command that
  tells me which layer said no, so I am not reading firewall rules on four hosts
  at 3am.

**Cluster operator**

- When I turn this on, I want to know exactly what it costs per connection and
  what happens when a handler dies, so I can decide whether it goes near the
  hot path.

**Autonomous agent** (drives the CLI/MCP without a human reading output)

- When I am handed a sandbox, I want to read its effective network policy as
  structured data, so I can tell the user "that host is blocked" instead of
  retrying a hanging connection.

### User scenarios the design must serve

**S1 — The locked-down agent sandbox.** A per-user agent sandbox may reach
`*.pypi.org` and `api.github.com/repos/**` and nothing else. It has no network
device; there is no route to anything the chain did not open. Denials appear in
the event stream tagged with the user. The operator adds a host to the allow
list by writing one KV key; every live sandbox picks it up on its next
heartbeat, with no restart.

**S2 — Egress content filtering.** The same sandbox is allowed to call
`api.openai.com`, but any request body carrying something that looks like a
cloud credential is rejected before it leaves the node, and the rejection names
the rule.

**S3 — mTLS ingress with real identity.** An internal API declares
`ingress: { tls: { mode: 'mutual' } }`. Legion terminates, verifies the client
certificate, and rejects callers whose identity is not in `service.allow` —
cryptographically, not by omitting them from a feed.

**S4 — Bring your own proxy.** A team already has a Go proxy that does their
custom audit format. They deploy it as an ordinary Legion service and name it as
a chain hop. Legion hands it connections with PROXY v2 metadata; the team writes
no Legion-specific code.

---

## 4. Goals

1. **One primitive, both directions, every runtime.** The same chain code serves
   ingress and egress, `exec` / `container` / `sandbox`, and the existing
   `connect.upstreams` path.
2. **TCP, UDP, and HTTP**, with Legion owning framing so handlers do not
   reimplement it.
3. **mTLS terminated and originated by Legion**, with the verified peer identity
   handed to handlers as metadata. A handler never implements TLS.
4. **A declarative rules surface a person can write in a jobspec** — no code, no
   build step, no image — plus an escape hatch that is an ordinary Legion
   service or sidecar in any language.
5. **Unbypassable egress for sandboxes**, by construction rather than by
   firewall configuration.
6. **Policy that changes without restarting the workload.**
7. **Per-identity observability** on a path Legion already owns.
8. **Zero cost when unused.** A job with no chain must produce byte-identical
   wire traffic, allocate nothing extra, and run no extra listeners.

## 5. Non-goals

- **A general L7 router.** Retries, timeouts, canary splitting, and outlier
  ejection stay with the planned waypoint
  ([service-discovery-mesh-and-config.md](service-discovery-mesh-and-config.md)
  Part C). The chain is a policy/observation path, not a traffic manager.
- **HTTP/2 and HTTP/3.** `protocol: 'http'` is HTTP/1.1 in this plan. h2 needs a
  multiplexed handler contract; QUIC is opaque UDP. Both are rejected at submit
  with a message naming the limitation, not silently mishandled.
- **WASM and `cdylib` handlers.** §9 Phase 8 argues why these come last, not
  first.
- **Transparent egress capture for `container` and `exec`.** Without netns those
  are cooperative (proxy env vars). §7.3's honesty table states this to the user
  rather than implying enforcement Legion does not have.
- **Replacing CNI, or building an overlay network.** The chain rides on whatever
  datapath exists; the datapath plugin remains separate future work.
- **Deep packet inspection or IDS.** Rules match declared fields only.
- **Cross-node ingress load balancing / a cluster edge gateway.** The chain is
  node-local, in front of one allocation.

---

## 6. Background — What Exists Today

Everything cited here is shipped code, not plan.

| Thing | Where | State |
| --- | --- | --- |
| `network.ports` (static + dynamic 20000–32000), assigned in-transaction at placement | [`assign_allocation_ports`](../../src/structs/network.rs) `network.rs:383`, called from `store.rs:7365` `assign_allocation_network` | Shipped. Statics claimed first across task ports **and** upstream loopback ports, then dynamics from one shared cursor — so any second port this plan needs comes from the same set with no new collision machinery |
| `network.mode` (`auto` \| `host`) | `network.rs:49` | **Parsed, stored, never read by anything.** A pre-carved extension point |
| `AllocNetwork { address, ports, upstreams }` persisted on the allocation, shipped to the worker | `network.rs:232`, proto `TaskAllocation.network = 14` | Shipped |
| `LEGION_PORT_*` / `LEGION_ADDR_*` / `LEGION_UPSTREAM_*` env injection | `AllocNetwork::env_pairs()` `network.rs:260` → `allocation_env()` `task_plugins/mod.rs:61` | Shipped; all three runtimes call it |
| Egress datapath: one loopback listener per `(allocation, upstream)` | [`src/worker/forwarder.rs`](../../src/worker/forwarder.rs); per-connection path `forward_connection()` :382, splice at `copy_bidirectional` :444 | Shipped. **No metrics, no policy, no visibility** |
| Balancer registry — one `const` table is the only name list, validation reads it, name travels on the wire | `src/worker/balance.rs:83` `BALANCERS`; no-drift test at `network.rs:1059` | Shipped. **The pattern this plan copies for handlers** |
| Fail-static endpoint feed on the heartbeat | `HeartbeatReply.upstream_endpoints`; built by `server.rs:7412` `build_upstream_endpoints` (returns `Vec`, never `R<_>`); applied at `daemon.rs:806` `apply_upstreams` only after a successful heartbeat | Shipped. **The delivery mechanism this plan reuses for policy** |
| `service.allow` intentions | `network.rs:131` `allows_job` — plain job-id string match, control-plane filtering only | Shipped, not cryptographic |
| `lifecycle: sidecar` — a peer task co-scheduled in the same group | `job.rs:399` `TaskLifecycle`; group rules at `task_plugins/mod.rs:745` | Shipped. **No ordering guarantee** — nothing makes a proxy start before the app |
| Workload identity: manager as OIDC IdP, short-lived ES256 JWTs, `sub` template `{region}:{job}:{group}:{task}` | `src/structs/workload_identity.rs:80`, `src/manager/workload_identity.rs` | Shipped. **There is no workload CA and no certificate issuance** — `src/certs.rs` is a dev-TLS fixture generator, dead code behind `#[allow(dead_code)]` |
| KV | `PutKv`/`GetKv`/`ListKv`/`DeleteKv`/`PatchKv` + locks | Shipped. **There is no `WatchKv` and no KV broadcast channel.** "Watch" in `docs/kv-store.md` means the *manager* reconciles server-side: `queue_kv_change` (`server.rs:5839`) → debounce → `reconcile_kv_changes` (`server.rs:2136`) |
| Volumes as the precedent for a worker-core resource | mounted before `plugin.prepare()` at `task_manager.rs:972`, handed to runtimes as typed data on `TaskInstance.volumes` | Shipped. **The shape this plan follows for capture setup** |
| `sandbox` runtime: OCI image in a microVM, Legion-built ext4 root disk, generated `/legion/init` | `src/task_plugins/sandbox.rs`, `microvm.rs` | Shipped. `network: { mode: 'none' \| 'tap' }` only; nothing maps host ports into the VM |
| `container` on Linux | `src/task_plugins/oci_bundle.rs:395` | **Host network namespace**, asserted by test at :669 |
| `TaskPlugin` trait — every method except `name()` has a default | `task_plugins/mod.rs:268` | Shipped. Adding a capability method is backward-compatible for out-of-tree plugins |
| Metrics registry (hand-rolled), 256-label-combo cap per family | `src/manager/telemetry.rs:616` `Metrics`, worker registry `WorkerMetrics` :980 | Shipped. **The cap is why §7.6 keeps user/host/path off metrics** |
| Event stream: `drafts` catalog → `EventRecorder` → `Store::emit_event` → broadcast | `src/storage/events.rs:167`, `store.rs:6189` | Shipped. Artifacts and volumes are precedent for a subsystem adding kinds |

**Two seams, and the asymmetry between them.** Egress has exactly one seam and
it is a single line — the `copy_bidirectional` at `forwarder.rs:444`; everything
above it (spec reconcile, endpoint feed, balancing) is already
idempotent-and-reconciled and needs no change. **Ingress has no seam at all**:
the catalog advertises the task's own host port and consumers dial it directly.
Creating one means Legion binding the advertised port while the task binds
another — which is why §7.5 exists and why ingress is a later phase than egress.

---

## 7. Proposed Design

### 7.1 The primitive: a chain

An allocation's datapath becomes an ordered list of hops on each side. The same
module drives both.

```
ingress:  peer ─► [Legion edge] ─► hop₁ ─► hop₂ ─► task's private port
                  binds the *advertised* host port; optional mTLS terminate

egress:   task ─► [Legion edge] ─► hop₁ ─► hop₂ ─► [dial] ─► destination
                  loopback proxy / vsock / upstream loopback port
```

A hop is one of two things, and that is the whole extension story:

- **a built-in handler** — resolved from a `const HANDLERS` table, exactly the
  `BALANCERS` pattern (`balance.rs:83`): one impl plus one row, the name travels
  on the wire, validation reads the same table so a name and an implementation
  cannot drift.
- **an external proxy** — any Legion service, or a sidecar task in the same
  group. Legion connects to it and hands it metadata; it is an ordinary program
  that reads a socket and writes a socket.

The terminal hop is the dialer. On egress that is the existing balancer +
endpoint list; on ingress it is the task's private port. Nothing else about the
chain differs between the two directions.

### 7.2 The jobspec surface

Progressive disclosure, matching the grammar `service` already uses (one-liner →
block). **The floor is one line and no code:**

```jsonnet
// Deny all egress except these. TCP/SNI matching.
network: {
  ports: [{ name: 'http', to: 8080 }],
  egress: { allow: ['*.pypi.org', 'files.pythonhosted.org', 'api.github.com'] },
}
```

**HTTP path rules — `protocol` must be explicit; nothing is inferred:**

```jsonnet
network: {
  labels: { user: 'alice', tier: 'free' },      // attribution, see §7.6
  egress: {
    protocol: 'http',
    policy: 'deny',                              // fallthrough verdict
    allow: ['*.pypi.org', 'api.github.com/repos/**'],
  },
}
```

Submitting a path in a `tcp` chain is rejected at validation, not silently
truncated to the host:

```
egress.allow[1] 'api.github.com/repos/**' matches on a path, but
egress.protocol is 'tcp'. Set protocol: 'http' to match on paths, or
drop the path to match the host only.
```

**The full block** — `allow` is sugar for a single `rules` hop, so the two forms
share one engine:

```jsonnet
egress: {
  protocol: 'http',
  policy: 'deny',
  onError: 'deny',                               // deny | allow | skip
  accessLog: true,
  rulesFrom: 'kv://net/policy/sandboxes',        // merged under the inline rules
  chain: [
    {
      handler: 'rules',
      rules: [
        { match: { host: '*.pypi.org' },                              action: 'allow' },
        { match: { host: 'api.github.com', path: '/repos/**',
                   method: ['GET'] },                                 action: 'allow' },
        { match: { header: { authorization: '*' } },                  action: 'deny',
          reason: 'credentials must not leave the sandbox' },
        { match: { host: 'api.openai.com' },                          action: 'redact',
          redact: { body: { pattern: '(AKIA|sk-)[A-Za-z0-9]{16,}' }, replace: '[REDACTED]' } },
      ],
    },
    { handler: 'proxy', service: 'egress-audit', onError: 'allow', timeout: '2s' },
  ],
}
```

First match wins; `policy` is the fallthrough. `redact` is a
transform-and-continue action, so it composes with a later `allow`.

**Ingress, with mTLS:**

```jsonnet
network: {
  ports: [{ name: 'api', to: 8080 }],
  service: { port: 'api', allow: ['checkout', 'billing'] },
  ingress: {
    protocol: 'http',
    tls: {
      mode: 'mutual',                            // off | server | mutual
      certFrom: 'legion',                        // legion | { certPath, keyPath }
      clientCa: 'legion',                        //   or a path / '${artifact.ca}'
      requireIdentity: ['prod:*:checkout:*', 'prod:*:billing:*'],
    },
    chain: [{ handler: 'rules', rules: [
      { match: { path: '/admin/**' }, action: 'deny', reason: 'not externally reachable' },
    ] }],
  },
}
```

`requireIdentity` patterns match the workload-identity `sub`, so the ingress
chain is where `service.allow` finally becomes cryptographic rather than a
control-plane omission.

**Bring your own proxy** — the handler is an ordinary service:

```jsonnet
egress: { chain: [{ handler: 'proxy', service: 'egress-guard' }] }
```

or a sidecar in the same group, addressed by its port label:

```jsonnet
tasks: [
  { name: 'app',   group: 'g', network: {
      egress: { chain: [{ handler: 'proxy', port: 'guard' }] } } },
  { name: 'guard', group: 'g', lifecycle: 'sidecar',
    network: { ports: [{ name: 'guard' }] } },
]
```

**Cluster-wide posture** (manager config — a job author cannot opt out):

```jsonnet
// configs/manager.jsonnet
network: {
  defaultEgress: { policy: 'deny', allow: ['*.internal.example.com'] },
  allowTlsInspect: false,          // §7.7 MITM is off unless an operator enables it
  handlerTimeout: '2s',
}
```

The effective chain is `cluster prefix + job chain`; a job may add hops and add
denials, never remove a cluster hop or widen a cluster denial. That is the
security admin's job-to-be-done, and it is enforced at merge, not by convention.

### 7.3 Capture — how traffic actually reaches the chain

This is the part every comparable system is vague about, so the plan is explicit
and **this table ships verbatim in the public doc.**

| Runtime | Egress capture | Ingress capture | Can the workload bypass it? |
| --- | --- | --- | --- |
| `sandbox`, `net: 'gated'` (new) | **No NIC in the guest.** A guest agent offers SOCKS5 + HTTP CONNECT on guest loopback and tunnels every stream over `vsock` to the node chain | node chain listens on the host port, forwards over `vsock` to the guest port | **No.** There is no other device and no route |
| `sandbox`, `net: 'tap'` | operator's TAP; chain is the default route | DNAT to the chain | Only with guest root *and* a gap in the operator's own rules |
| `container` | proxy env vars (`HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY`/`LEGION_EGRESS_PROXY`) — **cooperative**, because Linux containers run in the host netns today | Legion binds the advertised port, container publishes the private one | **Yes**, for egress |
| `exec` | proxy env vars — **cooperative** | Legion binds the advertised port, task binds the private one | **Yes**, for egress |
| any, `connect.upstreams` | the existing loopback forwarder — the task can only reach the upstream through it | n/a | No |

Ingress is enforceable everywhere, because Legion owns the advertised port and
the task's real port is dynamic and unadvertised. **Egress enforcement is a
property of the runtime, and the doc says which one you have.** An operator who
needs unbypassable egress runs `sandbox` with `net: 'gated'`; anyone else gets a
cooperative control that is honest about being cooperative.

**Why `gated` is the headline.** It needs no `CAP_NET_ADMIN`, no netns, no
bridge, no `iptables` on the host, and no per-node network plan. The enforcement
comes from the absence of a device, not from the correctness of a rule set —
which is the difference between a security boundary and a configuration you can
get wrong. Legion already builds the guest root disk and generates
`/legion/init` (`microvm.rs`), so shipping a small static `legion-guest-agent`
into it is a change to an existing build step. Both supported hypervisors expose
`vsock`.

The guest agent additionally installs a **guest-side** redirect so processes
that ignore `HTTP_PROXY` are still captured. That is guest-internal `nftables`
inside a VM Legion built — no host privilege, no host-side state.

`legion-guest-agent` is not a new invention this plan needs to justify alone:
[sandbox-snapshots-and-fast-start-plan.md](sandbox-snapshots-and-fast-start-plan.md)
already requires one to reconfigure a forked VM's identity on resume, and
`docs/task-runtime-sandbox.md` names a vsock guest agent as the missing piece
for `exec` into a microVM. Three features want the same component.

**Runtime integration.** `TaskPlugin` gains one capability method with a default,
so out-of-tree plugins keep compiling:

```rust
fn network_capture(&self) -> NetworkCapture { NetworkCapture::ProxyEnv }
//                  Unsupported | ProxyEnv | Vsock { .. } | PortMap
```

Capture is set up in the worker core **before `plugin.prepare()`**, exactly
where `mount_allocation_volumes` sits (`task_manager.rs:972`), and torn down
where `unmount_for_allocation` sits (:867). Runtimes receive it as typed data on
`TaskInstance`, the way they receive `volumes` — no runtime learns to speak the
chain protocol.

### 7.4 The external handler contract — no SDK

Legion connects to the handler and speaks the chain's protocol. Metadata rides
in the standard place for each:

- **TCP / UDP** — **PROXY protocol v2** header, with Legion TLVs: allocation id,
  job, group, task, `network.labels`, verified peer identity (ingress), and the
  requested destination (egress). Envoy, HAProxy, and nginx already parse this.
  `proxyProtocol: false` opts out for handlers that cannot.
- **HTTP** — request headers `Legion-Alloc-Id`, `Legion-Job`, `Legion-Task`,
  `Legion-Label-<Key>`, `Legion-Peer-Identity`, `Legion-Destination`. The
  handler is an ordinary forward proxy (egress) or reverse proxy (ingress); to
  deny, it returns a 4xx itself.

A handler declares nothing and registers nothing. `legion net handler test
<service>` runs a conformance suite against it — the same shape as
`legion volume driver test`, and the same reason: a contract nobody can verify
is a contract nobody implements correctly.

**Ordering.** Legion cannot guarantee a sidecar handler starts before the app
(`lifecycle: sidecar` has no ordering guarantee today —
`task_plugins/mod.rs:745` only enforces group membership). The chain therefore
**fails closed per `onError`** until the hop answers: the app starts, and its
traffic is denied rather than escaping unfiltered. Stated as a known limitation
in the public doc, with `onError: 'allow'` as the explicit opt-out for anyone
who prefers availability.

### 7.5 Ingress and the port swap

With an ingress chain, Legion binds the **advertised** host port and the task
binds a **private** one from the same per-node used-set
(`assign_allocation_ports`, `network.rs:383` — statics first, then one dynamic
cursor, so no new collision machinery).

`LEGION_PORT_<label>` keeps meaning *"the port you bind"* and simply holds the
private value. The invariant every existing job relies on stays true whether or
not a chain exists; the catalog, `resolve`, and health checks all target the
advertised port. `AllocPort` gains `private_port: Option<u16>`.

**Health checks therefore traverse the chain.** That is correct — a check should
test what a consumer experiences — and it is a footgun worth naming: a
deny-everything ingress rule will fail your own health check and roll your
deployment back. `legion net doctor` names this case specifically.

### 7.6 Observability, and the cardinality problem

"Per-user o11y" and "bounded metric cardinality" are in direct tension. The
metrics registry caps a family at 256 label combinations and folds the overflow
into `"other"` (`docs/metrics.md`). Putting a user, a hostname, or a path in a
metric label would blow that cap on the first thousand sandboxes and silently
destroy the metric. So the two are split:

- **Metrics — bounded labels only** (`WorkerMetrics`): `legion_net_conns_total{direction,protocol,verdict}`,
  `legion_net_bytes_total{direction}`, `legion_net_denied_total{direction}`,
  `legion_net_handler_seconds{handler}`, `legion_net_chain_errors_total{handler,kind}`.
  Job and task labels only where the job count is bounded by the deployment, and
  never user or destination.
- **Events — the audit trail.** New kinds `net_denied` (warn) and
  `net_chain_failed` (error), scoped to job + allocation, carrying destination,
  matched rule, and identity in `meta`. This is what answers "who called the
  expensive API" and "which rule blocked it".
- **Access log — the detail.** `accessLog: true` writes NDJSON into the alloc
  dir, picked up by the existing log pipeline. One line per connection or
  request, with `network.labels` on every line.

`network.labels` is the attribution key: an operator tags a sandbox
`{ user: 'alice' }` and every event and log line carries it, without it ever
becoming a metric dimension.

This also closes a standing gap: the connect forwarder emits **no metrics at
all** today. Phase 0 gives the existing datapath its first telemetry.

### 7.7 mTLS, and the TLS modes

Legion terminates and originates; handlers never see TLS.

| Mode | Where | What it does |
| --- | --- | --- |
| `off` | both | plaintext |
| `server` | ingress | Legion presents a cert |
| `mutual` | ingress | Legion presents a cert and **requires + verifies** a client cert; the peer identity goes to handlers and to `requireIdentity` / `service.allow` |
| `originate` | egress | task speaks plaintext to the chain; Legion adds TLS to the destination |
| `passthrough` | egress | **default.** TLS is opaque; rules match SNI and destination only |
| `inspect` | egress | Legion terminates, runs the chain on plaintext, re-encrypts. **This is a MITM** |

**Certificates come in two phases, and the first does not need a CA.**

- **Phase 5a — bring your own PKI.** `certPath`/`keyPath`/`clientCa` as files or
  `${artifact.<name>}`. Immediately useful, and it unblocks every operator who
  already has an internal CA. No new manager machinery.
- **Phase 5b — `certFrom: 'legion'`.** A cluster workload CA issues short-lived
  certs whose SAN is the workload-identity `sub`
  (`{region}:{job}:{group}:{task}`), delivered on the same worker path that
  already delivers identity JWTs (`src/worker/identity.rs`) and rotated by the
  same loop. This is genuinely new: **there is no workload CA today** —
  `src/certs.rs` is a dev-fixture generator, not an issuer. It is also the item
  the service-discovery plan has been waiting on, so it closes
  "mTLS between forwarders" as a side effect.

**`inspect` is off unless an operator turns it on** (`allowTlsInspect`), and it
requires the Legion CA in the workload's trust store — which is *only*
defensible for `sandbox`, because Legion builds that root filesystem. The public
doc will say the word "MITM", say which workloads it applies to, and say that
the sandbox's user can see the substituted issuer. It is the only way to serve
S2 (filter what egresses over HTTPS), and it should look exactly as serious as
it is.

### 7.8 Dynamic policy — the delivery mechanism already exists

`rulesFrom: 'kv://…'` is resolved by the **manager** and shipped in the
heartbeat reply as a `net_policy` snapshot, alongside `upstream_endpoints`, with
the same **fail-static** contract: policy is swapped only after a successful
heartbeat, so a manager outage keeps the last known good rules, and an explicit
empty set is authoritative.

There is no KV watch to build on — `WatchKv` does not exist and there is no KV
broadcast channel — but there is already a server-side reconcile: `PutKv` calls
`queue_kv_change` (`server.rs:5839`) into a debounce that drives
`reconcile_kv_changes` (`server.rs:2136`). Hooking that to bump a policy
generation means the next heartbeat carries the new rules. Worst case is one
heartbeat interval (2s), and **the workload is never restarted** — which is the
whole point of S1's 3am requirement. Unlike `kv.onChange`, net policy has no
`restart` or `rollout` action; live apply is the only behavior.

Cost control follows `build_upstream_endpoints` exactly: a fast path that
returns an empty vector without touching the store when no desired allocation
declares a chain, so nodes running plain jobs pay literally zero extra queries.

### 7.9 CLI, agent surface, and diagnostics

```
$ legion net doctor sandbox-alice
✓ egress chain declared        (protocol http, policy deny, 1 handler)
✓ capture is enforced          (sandbox net: gated — no guest NIC, vsock transport)
✓ handler 'rules' loaded       (7 rules, 3 from kv://net/policy/sandboxes, gen 41)
✗ handler 'egress-audit' has no healthy endpoints
  → onError is 'deny', so all egress is currently blocked
  → legion service doctor egress-audit
  → or set chain[1].onError: 'allow' to fail open
```

The `doctor` contract is the one `legion service doctor` already established —
walk the layers, stop at the first failure, print verdict + cause + fix hint.
Its layers here: chain declared → capture mode and whether it is enforced → each
handler resolvable and healthy → TLS material present and unexpired → recent
denials.

Also: `legion net policy <job>[/<task>]` (the effective chain after the cluster
merge and KV render — the one thing a client cannot compute itself), and
`legion net log <alloc>`. All three take `--json` through
`crate::cmd::output`, all three get rows in `catalog()`
(`src/cmd/capabilities.rs`), and `net policy` / `net doctor` become read-only
MCP tools. That is the agent persona's job-to-be-done: a hanging connection
becomes a structured "denied by rule 3".

---

## 8. Concrete Change Surface

**New:** `src/structs/netchain.rs` (`ChainConfig`, `HopConfig`, `RuleConfig`,
`RuleMatch`, `RuleAction`, `TlsConfig`, normalize + validate) and
`src/worker/netchain/` (`mod.rs` reconcile — mirroring `UpstreamForwarder::sync`;
`handler.rs` trait + `const HANDLERS` table; `rules.rs`; `tcp.rs` / `udp.rs` /
`http.rs`; `tls.rs`; `meta.rs` for PROXY v2 and `Legion-*` headers).

**Jobspec:** `NetworkConfig` gains `ingress`, `egress`, `labels`. Parsing goes
through `TaskConfigInput`/`normalize_task_network` (`job.rs:2251`), and the
`allow` sugar desugars to a `rules` hop there, so by validation time there is
one representation.

**Persistence: no migrations.** The new task fields ride inside the existing
`task_config.network` JSON column and the new allocation fields inside
`allocation.network`. `AllocPort` gains `private_port: Option<u16>`;
`AllocNetwork` gains `egress: Option<AllocEgress>`. Both are
`#[serde(default, skip_serializing_if)]`, so rows written before this plan parse
and re-serialize byte-identically — the same discipline `upstreams` used.

**Proto:** `TaskAllocation` gains `string task_net_chain` (empty string, not
`"{}"`, when unused — so non-chain jobs stay byte-identical on the wire, matching
`task_connect` at `dispatcher.rs:164`). `HeartbeatReply` gains
`repeated NetPolicy net_policies`. One new RPC, `GetNetPolicy` — needed because
the *effective* policy is cluster config + job chain + rendered KV, which only
the manager knows.

**ACLs:** `GetNetPolicy` is `AclResource::Job` / `Read` / `AclScope::job(...)` —
the policy is a property of a job, and reusing the existing vocabulary beats a
tenth resource. Cluster-wide posture lives in manager config, so it needs no
`write` endpoint. Per `AGENTS.md`: gate coarsely with `AclScope::any()` before
the job lookup, precisely after it, and add the row to
`tests/acl_endpoints.rs` — the `every_manager_rpc_is_classified` test fails the
build otherwise.

**Capture:** `TaskPlugin::network_capture()` (defaulted); `sandbox` config gains
`net: 'gated'`; `legion-guest-agent` built into the guest root disk; vsock
plumbed through `VmSpec` next to `TapNet`.

**Docs:** new public page `docs/network-handlers.md`, indexed in
`docs/README.md` and root `README.md`. Updates to `docs/service-discovery.md`
(the chain on connect), `docs/task-runtime-sandbox.md` (`gated`),
`docs/metrics.md`, `docs/event-stream.md`, `docs/agent-readiness.md`,
`docs/plugin-system.md` (the handler tiers and why `cdylib` is not one of them).
Full proto comments on every new message and field.
`configs/jobs/maximal.jsonnet` gains every new field as each phase lands, plus a
new `configs/jobs/sandbox-egress-demo.jsonnet`.

---

## 9. Implementation Phases

Each phase is independently shippable and independently valuable. **Egress comes
before ingress** because all three capabilities in the original request — per-user
observability, dynamic endpoint lockdown, filtering what leaves — are egress
capabilities; ingress serves the mTLS scenario, which needs the CA anyway.

| # | Phase | Ships on its own because… |
| --- | --- | --- |
| **0** | **Chain core + egress.** `netchain` module, `NetHandler` trait + `HANDLERS` table, built-in `rules` (TCP/SNI) and `log`, inserted at `forwarder.rs:444`. Plus the general egress listener (SOCKS5 + HTTP CONNECT) exported as `LEGION_EGRESS_PROXY` / `HTTP_PROXY` / `ALL_PROXY`. Metrics, events, access log. | `connect.upstreams` gains policy and its first telemetry; `exec` and `container` get a cooperative egress allow-list |
| **1** | **HTTP protocol mode.** HTTP/1.1 framing, host/path/method/header matching, `redact`, per-request metrics and events, `Legion-*` headers. h2/h3 rejected at submit with a named limitation. | "Lock down specific endpoints" and "filter what egresses" both become real |
| **2** | **Sandbox `net: 'gated'`.** Guest agent, vsock transport, guest-side redirect, `NetworkCapture`. | **The core use case is complete here** — unbypassable, policy-driven sandbox egress with per-user attribution |
| **3** | **Ingress chains.** Advertised/private port split, `AllocPort.private_port`, Legion binds the public port, `legion net doctor`, `net policy`, `net log`, capabilities + MCP. | Anything can now be fronted; unblocks the autoscaler's wake-on-request |
| **4** | **External handlers.** `handler: 'proxy'` by service and by sidecar port, PROXY v2 metadata, `onError`, timeouts, `legion net handler test`. | Bring-your-own-proxy in any language |
| **5** | **mTLS.** (a) BYO PKI: terminate, verify, originate. (b) `certFrom: 'legion'`: workload CA, SAN = identity `sub`, rotation on the existing identity path. | (a) is useful with no new manager machinery; (b) closes "mTLS between forwarders" and makes `service.allow` cryptographic |
| **6** | **TLS `inspect`** for sandboxes, gated on `allowTlsInspect`, CA injected into the guest trust store. | The only route to body-level filtering of HTTPS egress |
| **7** | **UDP.** Per-datagram chain with a flow table; destination rules and datagram-mode external handlers. No HTTP over UDP. | Completes the protocol matrix |
| **8** | **Sandboxed handlers (WASM).** Fuel/memory/deadline envelope per [service-discovery-mesh-and-config.md](service-discovery-mesh-and-config.md) Part D. | Deliberately last: Phase 4 already answers "run my own code" *out of process*, where a crash costs one connection instead of the node. In-process `cdylib` handlers are not planned at all — the datapath is `async`, the dylib ABI is hostile to `async` traits, and a panic there takes down every workload on the node |

---

## 10. Testing Plan

**Unit.** Rule matching (glob hosts, `**` paths, header globs, first-match-wins,
`policy` fallthrough); `allow` sugar desugaring to exactly the block form;
submit-time rejection of a path in a `tcp` chain, an unknown handler name, an h2
`protocol`, a `proxy` hop naming a nonexistent service; PROXY v2 encoding; the
handler-registry no-drift test (validation names derived from `HANDLERS`,
mirroring `network.rs:1059`).

**Datapath.** A chain with no handlers is a byte-exact passthrough; deny closes
without dialing; `redact` rewrites a streamed body across chunk boundaries;
`onError` honored for a handler that hangs, refuses, and returns garbage;
per-hop timeout bounds the connection.

**Capture.** A `gated` sandbox with a rule denying `example.com` cannot reach it
by hostname, by literal IP, by a raw socket, or by unsetting `HTTP_PROXY` — this
is the test that makes the unbypassability claim, and if it does not exist the
claim comes out of the doc.

**Ingress.** Advertised port serves through the chain while the private port is
what the task bound; catalog and `resolve` advertise the public port; a
deny-all ingress rule shows up as a *failing health check* and `net doctor`
names it.

**Policy.** KV write → new rules live within one heartbeat, no restart;
manager outage → last known good rules retained (fail-static); explicit empty
set → authoritative.

**mTLS.** Wrong CA rejected; expired cert rejected; identity not in
`requireIdentity` rejected with the identity in the event; rotation mid-connection
does not drop established connections.

**Regression — the load-bearing one.** A job with no chain must produce a
byte-identical `TaskAllocation`, start no listeners, and add no store queries.
Assert `build_upstream_endpoints`' zero-query fast path still holds with chains
in the tree.

**Benchmarks** (`benches/`, per `docs/testing-and-benchmarks.md`): connection
setup latency and throughput for passthrough vs. one `rules` hop vs. one
external hop; the number to defend is that a chain with no body filter still
reaches `copy_bidirectional` and costs nothing per byte.

---

## 11. Rollout and Backward Compatibility

- **Nothing changes for existing jobs.** Every field is optional; absent chain
  means the current code path, byte-identical on the wire, with no extra
  listener and no extra query. Legion has never had a network default, so there
  is no default to change.
- **No migrations.** New fields live inside existing JSON columns with
  `serde(default, skip_serializing_if)`.
- **Version skew.** An older worker receiving `task_net_chain` ignores an unknown
  field — so the manager must refuse to place a chained allocation on a worker
  that does not advertise chain support in its fingerprint. Silently running a
  workload with its egress policy dropped is the worst possible failure of this
  feature, and a placement-time constraint is the only acceptable answer. Per
  `docs/upgrades.md`: managers first.
- **Cluster posture is opt-in.** `network.defaultEgress` is unset by default; an
  operator who sets it is deliberately changing every job on the cluster, and
  `legion net policy` shows any job author the merged result.
- **`allowTlsInspect` defaults to false** and stays false through an upgrade.

---

## 12. Open Questions

1. **Should `network.labels` be a new field, or should the chain read
   `service.meta`?** Lean: a new field. `service.meta` is advertised in the
   catalog to every consumer, and `{ user: 'alice' }` is attribution data, not
   an advertisement.
2. **Does the general egress listener bind per-allocation loopback, or one
   per-node listener that demultiplexes by source?** Lean: per-allocation, for
   symmetry with the upstream forwarder and because it makes attribution
   trivially correct. Revisit if listener count becomes a real cost at high
   allocation density on one node.
3. **Should a `proxy` hop prefer node-local endpoints?** Lean: yes, once
   locality-aware balancing exists — an off-node hop per connection is a real
   latency cost. Until then, document it and let sidecar hops be the
   latency-sensitive answer.
4. **Where does `rulesFrom` land relative to inline rules?** Lean: KV rules
   evaluate *after* inline rules, so a job author cannot be silently overridden
   mid-flight — but cluster-config rules evaluate *first*, so an admin can be.
5. **Is `redact` on a streamed body worth the complexity in Phase 1?** A pattern
   spanning a chunk boundary needs a bounded rewrite buffer, which is the one
   place a handler can add unbounded memory. Lean: ship it with a hard buffer
   cap and a documented failure mode (`onError`) when a match would exceed it.

---

## 13. Acceptance Criteria

**Phase 0** — a `connect.upstreams` connection to a denied destination is closed
without a dial, the denial appears in the event stream and increments
`legion_net_denied_total`, and a job with no chain has byte-identical wire
traffic and no extra listener.

**Phase 1** — `egress: { protocol: 'http', allow: ['api.github.com/repos/**'] }`
permits `GET /repos/x/y` and denies `GET /user`, with the matched rule named in
the event; a path in a `tcp` chain is rejected at submit with the field name and
the fix.

**Phase 2** — inside a `gated` sandbox, a denied host is unreachable by
hostname, by literal IP, by raw socket, and after `unset HTTP_PROXY`; a `pip
install` from an allowed index succeeds; `legion net doctor` reports capture as
enforced.

**Phase 3** — a task with an ingress chain is reachable on its advertised port,
`legion service resolve` returns that port, and a deny-all ingress rule surfaces
as a failing health check that `net doctor` names.

**Phase 4** — an unmodified third-party proxy deployed as a Legion service works
as a hop and reads its metadata from the PROXY v2 header;
`legion net handler test` passes against it; killing it blocks traffic under
`onError: 'deny'` and passes it under `'allow'`.

**Phase 5** — a client without a cert is rejected; a client with a cert whose
identity is outside `requireIdentity` is rejected with the identity in the
event; cert rotation does not drop established connections.

**Every phase** — `cargo fmt --all --check`, `cargo clippy --all -- -D warnings`,
`cargo test --all --locked` green; the public doc updated in the same change as
the code; `internal_docs/TODO.md` updated in the same change.
