# Plan: Network Handlers — Ingress and Egress Policy, Capture, and Middleware

> **Judgement calls:** [`calls/network-handlers-plan_calls.md`](../calls/network-handlers-plan_calls.md)

> **Unified into [service-mesh-plan.md](service-mesh-plan.md), which
> sequences this plan's phases with the mesh and wins where the two differ.**
> The one difference: `network.capture` here is `network.mode = "gated"`
> there (the pre-carved datapath selector), and `proxyEnv` is what
> `mode = "host"` does when a policy is attached. Everything else in §7 is the
> detailed design the unified plan points at.
>
> **Status: SHIPPED (as phases M1–M9 of the unified plan; see its header
> for the two named deferrals).** Revision 2. The first revision was
> reviewed against the codebase, against a measured L4-vs-L7 cost study
> (§2), and against the follow-up questions "how do I get this on every task
> without writing it in every job" and "does posture scope to a pool or a
> namespace". Six decisions did not survive; the calls doc records each
> reversal. This plan supersedes revision 1 entirely.
>
> Composes with, and does not supersede,
> [service-discovery-final-plan.md](service-discovery-final-plan.md): the
> `connect.upstreams` forwarder is the datapath this plan generalizes, and
> `connect.upstreams` is where explicit egress lives (§7.4). Composes with
> [edge-boot-plan.md](edge-boot-plan.md): its `boot` gate becomes a real
> boundary on every runtime once the datapath is the only path (§7.3).
>
> **Delivers items other plans are blocked on:** "mTLS between forwarders"
> (service discovery §8 item 3); "ingress proxy" (TODO § Networking); the
> ingress dependency [autoscaler-plan.md](autoscaler-plan.md) names for
> wake-on-request scale-to-zero; edge-boot's deferred "per-destination network
> policy for the gated state"; and the vsock guest agent that both
> [sandbox-snapshots-and-fast-start-plan.md](sandbox-snapshots-and-fast-start-plan.md)
> (fork identity) and microVM `exec` are waiting on.
>
> **Deliberately deferred, by name:** TLS interception (MITM), WASM handlers,
> `cdylib` handlers, HTTP/2 and HTTP/3, transparent (non-proxy-env) capture
> inside a gated namespace, a cluster-edge load balancer, rate limits and
> quotas. §5 says why for each.

---

## 1. The Problem

A sandbox is a microVM running somebody else's code. The operator running a
fleet of them needs to answer three questions about every byte crossing the
boundary — *who sent it, was it allowed, what was in it* — and today Legion
cannot answer any of them.

**Today, concretely:**

- A `sandbox` task has `network = { mode = "none" }` (loopback only) or
  `mode = "tap"` — attach to a **pre-created** host TAP device Legion refuses
  to make because it needs `CAP_NET_ADMIN` and a host addressing plan
  ([src/task_plugins/sandbox.rs:121](../../src/task_plugins/sandbox.rs)). No
  network, or the operator's whole network. Nothing between.
- `container` on Linux runs in the **host network namespace** — a test
  asserts it ([src/task_plugins/oci_bundle.rs:779](../../src/task_plugins/oci_bundle.rs)).
  `exec` is a host process. Same all-or-nothing.
- The one datapath Legion owns —
  [`src/worker/forwarder.rs`](../../src/worker/forwarder.rs) behind
  `connect.upstreams` — splices sockets and emits **no metrics**. It cannot
  deny, cannot see a request, cannot say who talked to whom.
- `service.allow` intentions are enforced by *omitting* endpoints from a
  heartbeat feed. The public doc says so: it "does not stop a task that
  already knows a backend's `address:port`"
  ([docs/service-discovery.md](../../docs/service-discovery.md)).
- **There is no ingress path.** The catalog advertises the task's own host
  port and consumers dial it directly.
- Edge boot's `boot` gate is link state on one TAP device for `sandbox`, and
  for `container`/`exec` it is "don't create the upstream listeners yet" — the
  plan says so itself: "a `boot`-gated `exec` task can still reach the network
  through the host's own stack"
  ([edge-boot-plan.md §6.7](edge-boot-plan.md)). Per-destination policy for
  the gated state is deferred to "a networking feature."
- Tasks survive a worker restart (`restore_allocations`,
  [src/worker/task_manager.rs:2138](../../src/worker/task_manager.rs)); the
  forwarder does not (`UpstreamForwarder` is rebuilt at
  [daemon.rs:328](../../src/worker/daemon.rs), its `Drop` aborts every
  listener). **The workload outlives its network.** Today that is a papercut.
  Once the network path is a security boundary, it is the boundary
  disappearing during an upgrade.

**What an operator does instead.** `mode = "tap"`, a hand-built bridge,
`nftables` on every worker, a log pipeline off `conntrack`, none of it in the
jobspec, none of it visible to `legion job plan`, all of it in a Chef recipe.

**What this plan builds.** Three concepts, deliberately kept apart:

1. **Policy** — declarative rules, evaluated at the cheapest point that can
   decide them, stored as first-class objects that scope to a pool, a
   namespace, a runtime, or a label, and that change live.
2. **Capture** — how a workload's traffic is made to reach Legion's
   datapath. One mode, `gated`, defined by *what the workload's network
   namespace contains*, implemented per runtime, unbypassable on Linux for
   all three.
3. **Handlers** — code on the path, only when policy is not enough: built-in,
   a webhook, or any proxy in any language.

And one process that owns the bytes, `legion netd`, which outlives the
worker.

---

## 2. Prior Art

| System | What we take | What we reject |
| --- | --- | --- |
| **Tensorlake sandbox networking** ([blog](https://www.tensorlake.ai/blog/near-zero-overhead-sandbox-networking)) — measured: removing L7 interpretation took ingress from 1.12 to 2.07 GB/s and 0.90 to 0.50 CPU-s/GB; kTLS + `splice(2)` added only 2.07 → 2.50 | **The ratio.** L7 on the path costs ~2× throughput and ~45% CPU; crypto costs almost nothing by comparison. So: policy that can be decided at connect time never enters L7 (§7.1), the L4 path is `splice`, not a userspace copy (§7.12), and the datapath is a **standalone daemon** so sandbox connections survive orchestrator updates (§7.8). Their length-bounded preamble is our PROXY v2 | Their "no fallback, refuse to start" stance on kTLS. Legion's datapath doctrine is platform-identical behavior; kTLS is an optimization behind a probe, or it does not ship |
| **gVisor netstack, passt, slirp** — the guest has no host NIC; a userspace stack owns its traffic | The **absent-device** model as the security boundary (§7.3). Enforcement from the absence of a path, not the correctness of a rule set | A full userspace TCP stack in v1. Proxy-env capture inside the empty namespace first; a TUN + `smoltcp` agent later for tools that ignore `HTTP_PROXY` |
| **Envoy `ext_authz`, OPA, Kubernetes admission webhooks** | The **decision webhook** as the first external handler (§7.7): metadata in, verdict out, no bytes, cacheable. It is what every policy system converged on | `ext_proc`-style streaming body callbacks — a Legion-specific protocol. The byte path uses PROXY v2 and plain HTTP so a handler needs no SDK |
| **Envoy filter chains / Istio** | Ordered handlers; TLS and framing owned by the proxy, never the filter | The sidecar-per-workload lifecycle (the service-discovery plan §1 already rejected it; Istio retreated from it), and config that 503s when applied out of order |
| **Cilium** | Identity-keyed policy — rules and dashboards survive reschedules | Kernel dependence. Legion runs on macOS and without `CAP_NET_ADMIN` |
| **Kubernetes NetworkPolicy** | Match/action allow-list grammar; default-deny as an explicit posture; selector-scoped policy objects (§7.6) | L3/L4 only — cannot say `GET /repos/**`; silently a no-op without a CNI that implements it |
| **Istio/Linkerd injection** (`istio-injection=enabled` on a namespace) | Attach-by-selector so no job mentions it (§7.6) | The mutating webhook that synthesizes a container into every pod |
| **AI gateways** (Cloudflare AI Gateway, LiteLLM proxy) | The workload is *pointed at* the gateway; the gateway originates TLS; the body is visible without interception (§7.4) | A separate product to run — here it is an upstream |
| **Consul intentions + Connect** | Authorization on cryptographic identity; `service.allow` as the policy input | The sidecar; the second control plane |
| **HAProxy PROXY protocol v2** | The external handler metadata contract | — |

**The synthesis.** Nobody offers "run my policy on the sandbox's network path"
without a kernel dependency, a sidecar per workload, a second control plane,
or a MITM. Legion can, because it already owns a userspace datapath on every
node, already builds the sandbox root disk, already creates namespaces for
containers and hardened `exec`, already has a `network_gate` hook, and already
has identity, health, catalog, and heartbeat delivery. Most of §7 is wiring.

---

## 3. Jobs To Be Done

**Sandbox platform operator** — runs a fleet of per-user sandboxes.

- When a sandbox runs untrusted code, I want it to reach only the registries
  and APIs I named, so a prompt injection cannot exfiltrate to an address I
  did not approve — *and I want that to be true because there is no other
  path, not because a rule is correct.*
- When I discover a bad endpoint at 3am, I want to block it for every live
  sandbox in under a minute without restarting any of them.
- When I turn this on for a fleet, I want to declare it once for the pool,
  not in every job — and I want a job author to be unable to opt out.
- When finance asks who called the expensive API, I want per-user attribution
  of every outbound call.
- When a sandbox sends data to an approved endpoint, I want to strip what
  must never leave — *without decrypting my users' traffic to do it.*
- When I upgrade a worker, I want running sandboxes to keep their network.

**Job author / app developer**

- When I ship a service, I want "only these callers, over mTLS" in the same
  file as the rest of the job.
- When I already have a proxy that does something clever, I want it in the
  path without rewriting it as a plugin.
- When all I need is "ask my service whether this connection is OK", I want
  that to be a webhook, not a proxy I have to get right.

**Security / platform admin**

- When I set a rule for a pool or a namespace, I want it enforced whether or
  not the author opted in.
- When traffic was blocked, I want a record naming *which policy and which
  rule*, so I can tell a denial from a broken app.

**On-call responder**

- When a service cannot reach its dependency, I want one command that names
  the layer that said no.

**Cluster operator**

- When I turn this on, I want to know what it costs per byte, and what
  happens to running workloads when the datapath process restarts.

**Autonomous agent** — including the one *inside* the sandbox.

- When my request is blocked, I want the response to tell me what blocked it
  and why, so I can change my approach instead of retrying a hang.
- When I am about to call a host, I want to ask whether it is allowed
  without calling it.

### User scenarios

**S1 — The locked-down agent sandbox.** A per-user sandbox may reach
`*.pypi.org` and `api.github.com/repos/**`. Its network namespace contains
loopback and one Legion listener; there is no route to anything else. A
denied request gets a 403 whose body names the policy and rule. The operator
adds a host to the pool's policy and every live sandbox has it within one
heartbeat.

**S2 — Egress content filtering, no MITM.** The sandbox sets
`OPENAI_BASE_URL` to `http://$LEGION_UPSTREAM_OPENAI/v1`. It speaks plaintext
to a loopback port; Legion originates TLS to `api.openai.com`. A request body
carrying something shaped like a cloud credential is rejected before it
leaves the node, and the rejection names the rule. Nobody's TLS was broken.

**S3 — mTLS ingress with real identity.** An internal API declares
`ingress = { tls = { mode = "mutual" } }`. Legion terminates, verifies the
client cert, and rejects identities outside `service.allow` —
cryptographically.

**S4 — Bring your own decision.** A team's Go service answers "is this
connection OK?" over HTTP. They name it as an `authz` handler. Legion POSTs
metadata, caches the verdict per (identity, destination), and writes no
proxy code.

**S5 — Posture for a pool.** The admin applies one policy object with
`selector = { nodePools = ["untrusted"] }` and `enforcement = "require"`.
Every job that lands there gets it; no job can weaken it;
`legion net policy show <job>` shows any author the merged result and which
policy contributed each rule.

---

## 4. Goals

1. **One policy model, both directions, every runtime**, scoped to cluster,
   pool, namespace, runtime, job, or label; live-applied; never restarts a
   workload.
2. **Unbypassable egress on Linux for `sandbox`, `container`, and `exec`**, by
   construction rather than by firewall rules. Honest about macOS.
3. **Body-visible egress filtering without interception.**
4. **TCP, UDP, HTTP; mTLS terminated and originated by Legion.** A handler
   never implements TLS or framing.
5. **A rules surface a person writes in a jobspec, and an escape hatch that
   is a webhook or any proxy in any language.**
6. **Zero cost when unused; zero per-byte cost for allow-listed L4 flows;
   L7 only where a rule needs it.** Pinned by benchmark.
7. **Per-identity observability**, and denials an agent can read.
8. **The datapath outlives the worker.**

## 5. Non-goals

- **TLS interception (MITM).** §7.4 shows the non-MITM route to body
  filtering; MITM is deferred until a use case cannot be served by pointing
  the workload at an upstream. If it ever ships it is operator-gated and
  sandbox-only. Not in any phase here.
- **A general L7 router** — retries, timeouts, canary, outlier ejection stay
  with the planned waypoint.
- **HTTP/2 and HTTP/3.** `protocol = "http"` is HTTP/1.1; h2/h3 rejected at
  submit with a named limitation.
- **WASM and `cdylib` handlers.** The webhook and the proxy tiers answer "run
  my code" out of process, where a crash costs one connection.
- **Transparent capture inside a gated namespace.** v1 gated capture is
  proxy-env (`HTTP_PROXY`) plus DNS on loopback — a tool that ignores both
  simply has no route, which is the enforcement. A TUN + userspace stack that
  makes unmodified tools work is a later phase, not a v1 promise.
- **Raw UDP, ICMP, and undeclared listening ports inside a gated namespace.**
  Documented limitations; UDP arrives via SOCKS5 `UDP ASSOCIATE` in phase 7.
- **Replacing CNI; an overlay network; networking fingerprinters.** Separate
  future work; the datapath plugin remains the service-discovery plan's item.
- **Cluster-edge ingress / cross-node load balancing.** Ingress here is
  node-local, in front of one allocation.
- **Rate limits and quotas.** A natural rule action; not v1.
- **Lifting edge-boot's "gates above `stage` need no ports/upstreams" rule.**
  That is about port assignment in the apply transaction, not networking.

---

## 6. Background — What Exists Today

All shipped code.

| Thing | Where | What it gives this plan |
| --- | --- | --- |
| Port assignment in-transaction at placement, statics first then one dynamic cursor across task ports **and** upstream loopback ports | `assign_allocation_ports` [network.rs:420](../../src/structs/network.rs); `Store::assign_allocation_network` | Any extra port (private ingress port, egress proxy port) comes from the same set — no new collision machinery |
| `network.mode` — parsed, stored, **read by nothing** | `network.rs:49` | A pre-carved extension point |
| `AllocNetwork { address, ports, upstreams }` persisted and shipped; `env_pairs()` → `LEGION_PORT_*`, `LEGION_UPSTREAM_*` | `network.rs:232`, `:260`; proto `TaskAllocation.network` | Where private ports and the egress proxy address get injected |
| Egress datapath: one loopback listener per `(allocation, upstream)`; splice at `copy_bidirectional` | `forwarder.rs:382`, `:444` | The single seam; everything above it is reconciled and idempotent |
| Balancer registry — one `const` table, validation reads it, name on the wire | `balance.rs:83`; no-drift test `network.rs:1085` | The pattern for the built-in handler table |
| Fail-static heartbeat feed; zero-query fast path when nothing declares upstreams | `server.rs:8177` `build_upstream_endpoints`; `daemon.rs:1074` `apply_upstreams` | The delivery contract policy reuses |
| `lifecycle = "sidecar"` — co-scheduled peer, **no ordering guarantee** | `job.rs:414`; `task_plugins/mod.rs:1202` | Where a sidecar handler lives; why hops fail closed until healthy |
| `TaskPlugin::network_gate()` / `set_network_gate()` — edge-boot's hook; `Enforced` for `sandbox` (TAP link down via `ip link`), advisory for host runtimes | `task_plugins/mod.rs:710`, `:720`; `sandbox.rs` `set_tap_link` | The hook `netd` implements: for a gated allocation the gate is "policy withheld," on every runtime |
| Workload identity: OIDC JWTs, `sub` = `{region}:{job}:{group}:{task}`; **no CA, no cert issuance** (`src/certs.rs` is a dev fixture) | `structs/workload_identity.rs:90`; `manager/workload_identity.rs` | Identity for attribution and `requireIdentity`; the CA is phase 6b's new work |
| Keyed stateful instances, `LEGION_INSTANCE_ID` | `docs/stateful-workloads.md` | Per-user sandboxes are keyed instances; the key is free attribution |
| KV: no `WatchKv`, no broadcast; server-side `queue_kv_change` → debounce → `reconcile_kv_changes` | `server.rs:2040`, `:2384` | Why policy rides the heartbeat, not a watch |
| ACL policies: stored, replicated, `legion acl` CLI, `AclRule` selectors for `namespaces` / `names` / `pools` | `structs/acl.rs:103`, `:186` | The precedent and the selector vocabulary for policy objects |
| `node.pool` — real registration field, default `"default"`, ACL-scopable; **not in `config_attrs`**, so `count.per = "pool"` fails today | `daemon.rs:310`; `scheduler.rs:4320` `node_attribute` | Pool scoping is real; per-node handler jobs need two lines in `daemon.rs` |
| Namespaces **do not exist** — "jobs live in `default` until namespaces land" | `docs/auth-and-acls.md:75` | Selector accepts `namespaces` now, matches everything, needs no migration later |
| Volumes as worker-core resource: mounted before `plugin.prepare()`, typed data on `TaskInstance` | `task_manager.rs:1494` | The shape for capture setup |
| Tasks re-adopted after worker restart; forwarder rebuilt | `task_manager.rs:2138`; `daemon.rs:328`; `forwarder.rs:334` | The lifetime asymmetry `netd` fixes |
| `exec` hardening already unshares a **mount** namespace | `exec_isolation.rs:36` | Same privilege class as an empty **network** namespace |
| OCI bundle: `pid`/`ipc`/`uts`/`mount` namespaces, **network omitted**; `dns_mounts` bind-mounts host `resolv.conf` | `oci_bundle.rs:576`, `:393` | A gated container is one more namespace entry and a different `resolv.conf` |
| `sandbox`: Legion-built ext4 root, generated `/legion/init`; `none`/`tap`; vsock named as the missing piece for `exec` | `sandbox.rs`, `microvm.rs:208` | Where the guest agent goes |
| Metrics: 256-label-combo cap, overflow → `"other"` | `docs/metrics.md:42` | Why attribution is never a metric label |
| Events: `drafts` → `EventRecorder` → broadcast | `storage/events.rs:166` | The audit trail |

---

## 7. Proposed Design

### 7.1 Three concepts, kept apart

```
                 ┌──────────── policy ────────────┐
                 │ connect-time rules  (host/port/SNI/identity)  → decided before any dial
                 │ request-time rules  (method/path/header)      → decided at HTTP head
                 │ body rules          (dlp)                     → only inside a handler
                 └────────────────────────────────┘
capture ─►  [ legion netd edge ]  ─►  handler₁ ─► handler₂  ─►  dial / task
            evaluates policy;          code on the path,
            TLS; framing; PROXY v2     only if declared
```

**Policy is not a handler.** A rule is evaluated at the earliest point that can
decide it, and a chain with only connect-time rules never leaves L4: allow →
`splice`, deny → close, no bytes parsed. This is the design consequence of the
Tensorlake measurement, and it is what makes "deny everything but three hosts"
cost nothing per byte.

**Handlers are code on the path**, present only when declared. Built-ins come
from a `const HANDLERS` table (the `BALANCERS` pattern); external handlers are
a webhook or a proxy (§7.7).

**Capture is how bytes reach the edge** (§7.3), and is orthogonal to both.

### 7.2 The jobspec surface

Floor — one line, TCP/SNI, deny-by-default:

```nickel
network = {
    ports = [{ name = "http", to = 8080 }],
    egress = { allow = ["*.pypi.org", "files.pythonhosted.org", "api.github.com"] },
},
```

HTTP path rules — `protocol` is explicit, never inferred:

```nickel
network = {
    labels = { user = "alice", tier = "free" },
    egress = {
        protocol = "http",
        policy = "deny",
        allow = ["*.pypi.org", "api.github.com/repos/**"],
    },
},
```

A path in a `tcp` chain is rejected at submit:

```
egress.allow[1] "api.github.com/repos/**" matches on a path, but
egress.protocol is "tcp". Set protocol = "http" to match on paths, or
drop the path to match the host only.
```

The full block — `allow` is sugar for `rules`; `rules` and `handlers` are
separate keys because they are separate things:

```nickel
egress = {
    protocol = "http",
    policy = "deny",
    onError = "deny",                 # deny | allow — what a failed handler means
    accessLog = true,
    rules = [
        { when = { host = "*.pypi.org" },                                    action = "allow" },
        { when = { host = "api.github.com", path = "/repos/**", method = ["GET"] }, action = "allow" },
        { when = { header = { authorization = "*" } },                       action = "deny",
          reason = "credentials must not leave the sandbox" },
    ],
    handlers = [
        { handler = "authz", url = "http://policy.internal/check", cache = "30s" },
        { handler = "proxy", service = "egress-audit", timeout = "2s", onError = "allow" },
    ],
},
```

First match wins; `policy` is the fallthrough. `reason` is returned to the
workload (§7.11).

Ingress with mTLS:

```nickel
network = {
    ports = [{ name = "api", to = 8080 }],
    service = { port = "api", allow = ["checkout", "billing"] },
    ingress = {
        protocol = "http",
        tls = {
            mode = "mutual",
            certFrom = "legion",
            clientCa = "legion",
            requireIdentity = ["prod:*:checkout:*", "prod:*:billing:*"],
        },
        rules = [{ when = { path = "/admin/**" }, action = "deny", reason = "not externally reachable" }],
    },
},
```

Bring your own proxy — as a service, a sidecar, or the one on this node:

```nickel
handlers = [{ handler = "proxy", service = "egress-guard" }],   # any healthy endpoint
handlers = [{ handler = "proxy", port = "guard" }],              # sidecar task in this group
handlers = [{ handler = "proxy", node = "egress-guard" }],       # node-local endpoints only
```

Capture, per task (§7.3):

```nickel
network = { capture = "gated" },   # gated | proxyEnv | none (default depends on runtime)
```

### 7.3 Capture — `gated` is a property of the namespace, not of a runtime

Revision 1 made unbypassable capture a sandbox-only feature built on vsock.
The reconsideration: vsock is a transport. **The security property is "the
workload's network namespace contains only loopback and a listener Legion
owns."** That property has a per-runtime implementation, and Legion already has
the privilege for each on Linux:

| Runtime | How `gated` is implemented | What Legion already does that makes it a small change |
| --- | --- | --- |
| `sandbox` | **No NIC in the guest.** `legion-guest-agent` (static, built into the root disk) offers SOCKS5 + HTTP CONNECT on guest loopback and DNS on `127.0.0.1:53`, and carries every stream over `vsock` to `netd`. Ingress arrives over vsock in reverse | Legion builds the ext4 root and generates `/legion/init` (`microvm.rs`); both hypervisors expose vsock **as a host unix socket**, so `netd` needs no `AF_VSOCK` |
| `container` | **An empty network namespace, created and owned by `netd`**, that the container *joins* (`"namespaces": [{ "type": "network", "path": … }]`). `netd` binds the proxy and DNS listeners on loopback *inside* it (`setns`, bind, return) before the container starts. `resolv.conf` points at loopback | The bundle already creates four namespaces and omits this one (`oci_bundle.rs:576`); `dns_mounts` already bind-mounts a resolver |
| `exec` | Same empty namespace; the process is `setns`'d into it before `exec` | `hardening` already unshares a mount namespace (`exec_isolation.rs:36`) — same privilege, same code path |
| any, `connect.upstreams` | The existing loopback forwarder | unchanged |

**Inside a gated namespace the workload has `HTTP_PROXY` / `HTTPS_PROXY` /
`ALL_PROXY` / `LEGION_EGRESS_PROXY` pointing at the loopback listener, and a
resolver on loopback. A tool that honors those works; a tool that ignores them
has no route and fails.** That failure *is* the enforcement — nothing leaks. It
is also the honest limitation: v1 gated capture is proxy-env, not transparent,
and the doc says so. A later phase adds a TUN + userspace stack in the same
namespace so unmodified tools work; the security property is identical either
way, so it is an ergonomics phase, not a security one.

**Composition with the edge-boot gate.** `TaskPlugin::network_gate()` exists
today: `Enforced` for `sandbox` (the TAP link is held down), and for host
runtimes "don't create the upstream listeners yet" — which edge-boot's own doc
calls a weaker boundary, because "a `boot`-gated `exec` task can still reach
the network through the host's own stack." For a `gated` allocation, `netd`
implements the gate as **policy withheld**: the namespace exists and has no
route, and ratification is what delivers the first policy. That makes the
`boot` gate a real boundary on all three runtimes, and it delivers the
per-destination policy for the gated state that edge-boot deferred — a
`boot`-gated sandbox can be allowed the package registry before ratification
by giving it a policy with only that host. `mode = "tap"` sandboxes keep the
link-state gate they have.

**Honesty table — ships verbatim in the public doc:**

| Runtime / platform | Egress | Ingress | Bypassable? |
| --- | --- | --- | --- |
| `sandbox`, Linux, `gated` | no NIC; vsock to `netd` | vsock from `netd` | **No** |
| `container`, Linux, `gated` | empty netns; loopback listener owned by `netd` | `netd` dials the private port inside the netns | **No** |
| `exec`, Linux, `gated` | same | same | **No** (needs the same privilege as `hardening`) |
| any, macOS, or `capture = "proxyEnv"` | proxy env vars only — **cooperative** | Legion binds the advertised port; task binds private | **Yes**, for egress |
| `sandbox`, `mode = "tap"` | operator's TAP | DNAT | mostly |

**Defaults.** `capture` defaults to `gated` for `sandbox`, and to `proxyEnv`
for `container` and `exec` — a container that suddenly loses host networking
on upgrade is the kind of default change this repo does not make silently. A
policy object (§7.6) can `require` `gated` for a pool, which is how a sandbox
operator makes it fleet-wide.

**Runtime integration.** `TaskPlugin` gains one defaulted method,
`fn network_capture(&self) -> &'static [Capture]`, so out-of-tree plugins keep
compiling. Capture is set up by `netd` before `plugin.prepare()`, where
`mount_allocation_volumes` sits (`task_manager.rs:1494`), and torn down where
`unmount_for_allocation` sits. Runtimes receive it as typed data on
`TaskInstance` — the netns path, the proxy address, the resolver address — the
way they receive `volumes`.

### 7.4 Explicit egress: external upstreams, and why there is no MITM

`connect.upstreams` already means "dial a *name* through a loopback port the
forwarder owns." An external destination is the same shape with a static
target instead of a catalog service:

```nickel
connect = {
    upstreams = [
        { service = "db" },
        {
            name = "openai",
            to = "https://api.openai.com",
            handlers = [{
                handler = "dlp",
                body = { maxBytes = "1Mi", patterns = ["(AKIA|sk-)[A-Za-z0-9]{16,}"] },
            }],
        },
    ],
},
```

The task gets `LEGION_UPSTREAM_OPENAI=127.0.0.1:<port>` and speaks **plaintext
HTTP** to it. `netd` originates TLS to `api.openai.com` (SNI, host rewrite,
system or pinned CA). The request body is visible on the host — not because
anyone's TLS was intercepted, but because the workload was pointed at a
gateway, which is what every AI-gateway product does. In the sandbox:

```sh
export OPENAI_BASE_URL="http://$LEGION_UPSTREAM_OPENAI/v1"
```

This is the recommended route to S2 and it retires revision 1's TLS `inspect`
mode entirely. Legion never holds a CA that can impersonate the public
internet. External upstreams work for every runtime, gated or not, because
they are opt-in by address.

Upstreams and the egress chain are the two tiers of egress, stated plainly:

| | Explicit (`connect.upstreams`) | Transparent (`network.egress` + capture) |
| --- | --- | --- |
| Workload does | dials a loopback port Legion named | anything; the namespace routes it |
| Body visible | yes, always (plaintext on loopback) | only for plain HTTP; TLS is opaque (SNI/host rules only) |
| For | known APIs, LLM gateways, databases | `pip`, `git`, `curl`, the long tail |

### 7.5 DNS is part of egress policy

A gated namespace has no resolver unless Legion provides one, and DNS is both
the cheapest place to deny and a well-known exfiltration channel. So `netd`
serves DNS on loopback inside every gated namespace (and the guest agent does
inside the VM), forwarding over the same channel as traffic:

- Resolution happens on the host, through the host resolver.
- Connect-time rules are evaluated at resolution: a denied host returns
  `NXDOMAIN` **and** emits the same `net_denied` event a connection would, so
  denial is one round trip and never a hanging connect.
- Non-loopback DNS from inside the namespace has no route. TXT-record exfil to
  an attacker's authoritative server is closed by construction, not by rule.
- SOCKS5 clients that resolve remotely (`socks5h://`) get the same treatment
  at CONNECT.

Cooperative (`proxyEnv`) capture leaves the host resolver alone.

### 7.6 Policy objects: stored, selector-scoped, live

Revision 1 put cluster posture in `configs/manager.ncl`. That fails two ways:
in a raft group the effective posture depends on which manager leads, and it
cannot change without a restart, which defeats the 3am requirement. The ACL
system already solved this shape — stored, replicated, ACL'd, CLI-edited —
and policy follows it exactly.

```bash
legion net policy apply -f configs/policies/sandbox-egress.ncl
legion net policy list | describe <name> | delete <name>
legion net policy show <job>[/<task>]        # the merged, effective result
legion net policy test <job> --to https://api.github.com/user   # dry-run a verdict
```

```nickel
# configs/policies/sandbox-egress.ncl
{
    name = "sandbox-egress",
    "priority" = 100,
    selector = {
        nodePools = ["untrusted"],       # node.pool — real today
        namespaces = ["sandboxes"],      # matches everything until namespaces land
        runtimes = ["sandbox"],
        jobs = ["agent-*"],              # glob, like AclRule.names
        labels = { tier = "free" },      # task network.labels
    },
    enforcement = "require",             # require | default
    capture = "gated",
    egress = {
        protocol = "http",
        policy = "deny",
        allow = ["*.pypi.org", "api.github.com/repos/**"],
        accessLog = true,
    },
    ingress = { tls = { mode = "mutual", certFrom = "legion" } },
}
```

`selector = {}` matches every task — "on for the whole cluster" with no job
mentioning it.

| `enforcement` | Applied when | Job author may |
| --- | --- | --- |
| `default` | the task declares no chain of its own | replace it entirely by declaring one |
| `require` | always, **ahead of** the task's own | append rules and handlers, add denials; never remove or widen |

**Merge, deterministic and reviewable.** All matching `require` policies in
`priority` order, then exactly one `default` (highest priority; a tie is a
validation error at `apply`, never a runtime coin flip), then the task's own
block. **A task's inline `network.egress` is simply a policy object scoped to
that task, versioned with the job** — one representation, one merge function,
shared by `netd`, by `legion net policy show`, and by `legion job plan`. Every
rule in the merged output carries the policy and index it came from.

**Live.** The merge is computed on the dispatch/heartbeat path and **never
stored on the task_config**. Editing a policy re-scopes every running workload
within one heartbeat; a job submitted before the policy existed picks it up.

**Delivery — content-addressed.** A node with a thousand identical sandboxes
must not receive a thousand rule sets every two seconds. `HeartbeatReply`
carries `[{ allocation_id, policy_hash }]`; `netd` keeps a `hash → body` cache
and fetches a miss once via `GetNetPolicy(hash)`. Identical merged policies
share one hash and one fetch. Fail-static, exactly as `upstream_endpoints`:
swap only after a successful heartbeat, keep last-known-good on failure, an
explicit empty set is authoritative. **A gated allocation with no policy yet
has no network** — fail closed until the first policy arrives, which is why
`netd` readiness gates task start (§7.8) and why the edge-boot gate composes
for free (§7.3).

**Cost when unused.** The heartbeat builder short-circuits to an empty vector
without a store query when no desired allocation is matched by any policy —
the `build_upstream_endpoints` fast path, preserved.

**ACL.** A new `AclResource::Net`, scoped by pool and namespace, so a pool's
admin can set posture for their pool without cluster-wide write. `GetNetPolicy`
is worker-plane. Every RPC lands in `tests/integration/acl_endpoints.rs`.

**Bootstrap.** `network.seedPolicies` in `configs/manager.ncl`, applied once
at cluster init, so a fresh cluster is not open in the window before the first
`apply`.

### 7.7 Handlers: built-in, webhook, proxy

| Tier | Contract | Sees bytes | Use |
| --- | --- | --- | --- |
| **built-in** (`rules` is not one; these are `dlp`, `log`, later `ratelimit`) | `const HANDLERS` table; name on the wire; validation reads the table | as declared | ships in the binary |
| **`authz`** | `netd` POSTs JSON metadata (identity, labels, instance key, destination, and in HTTP mode the request head) to `url`; reply `{ verdict: allow\|deny, reason?, ttl? }`; verdict cached per (identity, destination) for `cache` | **no** | "ask my service" — the on-ramp; what most custom logic actually is |
| **`proxy`** | `netd` connects to it and speaks the chain's protocol: PROXY v2 header with Legion TLVs (TCP/UDP), or `Legion-*` request headers (HTTP). To deny, close or return 4xx | yes | any existing proxy, any language, no SDK |

`authz` exists because revision 1 made every custom decision cost a correct
HTTP proxy. A webhook that returns 200 or 403 is what Envoy `ext_authz`,
OPA, and admission webhooks all converged on; with a cached verdict it costs
one RPC per new (identity, destination) pair, not per connection.

**Every hop has `timeout` (default `handlerTimeout`, 2s) and `onError`.** There
is no syntax for an unbounded handler. Default `onError` is `deny` in both
directions.

**Ordering.** A sidecar handler may start after the app (`lifecycle = "sidecar"`
has no ordering guarantee). The chain fails closed until the hop is healthy;
the app comes up with its traffic denied rather than unfiltered. Stated in the
public doc; `onError = "allow"` is the explicit availability-first opt-out.

`legion net handler test <service|url>` runs the conformance suite for either
external tier — the `legion volume driver test` precedent.

### 7.8 `legion netd` — the datapath outlives the worker

The datapath is a **separate process from the same binary**, spawned and
supervised by the worker, holding every listener, every namespace, every
vsock socket, and the policy cache. The worker keeps control-plane duties only
— heartbeats, allocations, policy hashes — and talks to `netd` over a local
unix socket with a narrow interface: *policy in, telemetry out, capture
setup/teardown, gate open/close*.

Why a process, in this codebase's own terms:

- **Lifetime.** Tasks already survive a worker restart; now their network
  does too. A worker upgrade no longer severs every gated sandbox on the node.
- **Crash boundary.** A panic in an HTTP parser or a body filter kills `netd`
  and its connections — not the worker, not the tasks, not the heartbeat.
  `netd` is restarted by the worker; listeners are re-bound from the
  namespace paths and vsock sockets it persisted.
- **Upgrade.** On `netd` restart, the old process hands its listening
  descriptors to the new one (`SCM_RIGHTS`), so new connections never fail;
  in-flight connections drop, which is the same as any proxy restart and is
  documented.
- **One binary.** `legion netd` is a subcommand, not a second install. The
  repo's headline claim survives.

Two consequences that must be first-class:

- **Readiness gates start.** A gated task is not started until `netd` reports
  its namespace, listeners, resolver, and *a policy* ready — the volume
  precedent: "a task must never observe a missing data directory." Under
  edge-boot's `boot` gate the namespace and listeners are ready and the policy
  is withheld until ratification.
- **`netd` health is node health**, and an allocation whose capture path is
  down is marked unhealthy with a reason (`net_capture_unavailable`) so
  rollouts and `doctor` see it. A sandbox running with a dead network is not
  "healthy".

Phase 0 may keep the datapath in-process **provided the interface is the
`netd` interface from day one**, so the split in phase 3 is a process
boundary, not a redesign.

### 7.9 Ingress and the port swap

With an ingress chain, `netd` binds the **advertised** host port and the task
binds a **private** one from the same per-node used-set. `LEGION_PORT_<label>`
keeps meaning "the port you bind" and holds the private value; catalog,
`resolve`, and health checks target the advertised one. `AllocPort` gains
`private_port`. In a gated namespace, `netd` dials the private port from
inside the namespace; in a sandbox, over vsock.

**Health checks traverse the chain.** Correct — a check should test what a
consumer experiences — and a footgun: a deny-all ingress rule fails your own
health check and rolls you back. `net doctor` names that case.

### 7.10 mTLS, and the passthrough fast path

Legion terminates and originates; a handler never sees TLS.

| Mode | Where | Meaning |
| --- | --- | --- |
| `off` | both | plaintext |
| `server` / `mutual` | ingress | present a cert / present and **verify** the client's; verified identity feeds `requireIdentity`, `service.allow`, handlers |
| `originate` | egress, external upstreams | plaintext from the task, TLS to the destination |
| `passthrough` | egress | **default.** opaque; rules see SNI and destination |

Certificates in two phases: **6a bring-your-own** (`certPath`/`keyPath`/
`clientCa` as files or `${artifact.<name>}`) — useful immediately, no manager
machinery; **6b `certFrom = "legion"`** — a workload CA issuing short-lived
certs whose SAN is the identity `sub`, delivered on the existing identity path
(`src/worker/identity.rs`) and rotated by its loop. 6b is the outstanding
"mTLS between forwarders" item and makes `service.allow` cryptographic.

**kTLS + `splice` passthrough.** When ingress terminates mTLS and no handler
wants bytes, `netd` installs the negotiated keys with `setsockopt(TLS_TX/RX)`
and splices — mTLS at near-L4 cost. Linux 5.1+ with `CONFIG_TLS`, behind a
probe, with a userspace fallback that is behaviorally identical. The datapath
doctrine ("nothing here is platform-conditional", `forwarder.rs:12`) is kept by
making this an optimization the user cannot observe except in `doctor` output.

### 7.11 Observability, attribution, and denials an agent can read

Attribution keys, on every event and log line: job, task, allocation,
**instance key** (per-user sandboxes are keyed instances, so `alice` is
already there), `network.labels`, verified peer identity where present.
**Never on a metric label** — the 256-combination cap folds overflow into
`"other"` and silently destroys the series.

- **Metrics** (`WorkerMetrics`, bounded): `legion_net_conns_total{direction,protocol,verdict}`,
  `legion_net_bytes_total{direction}`, `legion_net_denied_total{direction}`,
  `legion_net_handler_seconds{handler}`, `legion_net_chain_errors_total{handler,kind}`,
  `legion_net_policy_generation`.
- **Events**: `net_denied` (warn; policy, rule index, destination, identity),
  `net_chain_failed` (error), `net_capture_unavailable` (error),
  `net_policy_applied` (info; hash, generation).
- **Access log**: `accessLog = true` → NDJSON in the alloc dir, one line per
  connection or request, into the existing log pipeline.

**Structured denials.** In HTTP mode a denial is a `403` with header
`Legion-Denied-By: sandbox-egress/2` and body
`{ "legion": { "denied_by": "policy/sandbox-egress rule 2", "reason": "…" } }`.
In TCP mode the connection is closed and the event carries the same fields.
An agent inside the sandbox reads *why* and changes its approach instead of
retrying a hang — the agent-readiness rule applied to the datapath.

**Diagnostics.** `legion net doctor <job>` walks: policy resolved (which
objects, which won) → capture mode and whether it is *enforced* → `netd`
ready → each handler resolvable and healthy → TLS material present → recent
denials. Verdict + cause + fix hint, the `service doctor` contract.
`legion net policy test <job> --to <url>` returns the verdict and the rule
without sending anything. All with `--json`, all in `catalog()`, `policy show`
/ `policy test` / `doctor` as read-only MCP tools.

### 7.12 Performance invariants — pinned, not promised

> **Update (datapath-performance plan):** invariants 2 and 3 are measured by
> `benchmarks/micro/net_bench.rs`, and the L4 path now splices on Linux.
> The benchmarks are run by hand and reported in PRs rather than failing CI;
> see [datapath-performance-plan_calls.md](../calls/datapath-performance-plan_calls.md)
> entry 8.

Stated as benchmarks in `benchmarks/micro/` that fail on regression:

1. **A task with no policy and no capture is byte-identical on the wire**,
   starts no listener, and adds no store query.
2. **An allow-listed L4 flow reaches `splice` with zero bytes parsed.** The
   only per-connection cost is the rule lookup at connect/resolve time.
3. **Passthrough throughput ≥ the current forwarder's**, and the L4/L7 gap
   is measured and published so anyone enabling `protocol = "http"` knows the
   price (Tensorlake's ~2× is the expectation).
4. **Policy delivery is O(distinct policies per node)**, not O(allocations).

---

## 8. Concrete Change Surface

**New modules.** `src/structs/netpolicy.rs` (`PolicyObject`, `Selector`,
`ChainConfig`, `RuleConfig`, `HandlerConfig`, `TlsConfig`, normalize +
validate + the deterministic merge); `src/netd/` (`main` subcommand,
`capture/{netns,vsock,proxyenv}.rs`, `edge/{tcp,udp,http,tls,dns}.rs`,
`policy.rs` cache + evaluation, `handlers/{mod,dlp,log,authz,proxy}.rs`,
`meta.rs` PROXY v2, `ipc.rs` the worker↔netd socket); `guest-agent/` a small
static crate for the sandbox root disk.

**Jobspec.** `NetworkConfig` gains `ingress`, `egress`, `capture`, `labels`.
`UpstreamConfig` gains `name`, `to`, `tls`, `handlers`. Parsing via
`TaskConfigInput` / `normalize_task_network` (`job.rs:2463`); `allow` sugar
desugars there.

**Persistence.** A `net_policy` table (name, priority, selector, body, version,
hash) with raft-replicated writes — the ACL policy table's shape. **No
migrations to `task_config` or `allocation`**: the new task fields ride in the
existing `network` JSON column, `AllocPort.private_port` and
`AllocNetwork.egress` with `serde(default, skip_serializing_if)`.

**Proto.** `TaskAllocation` gains `string task_net_capture` (empty when
unused, like `task_connect`). `HeartbeatReply` gains
`repeated AllocNetPolicy net_policies { allocation_id, policy_hash }`. RPCs:
`ApplyNetPolicy`, `ListNetPolicies`, `GetNetPolicyObject`, `DeleteNetPolicy`
(`AclResource::Net` write/read, scoped), `ShowNetPolicy` and `TestNetPolicy`
(`Net` read, job-scoped), `GetNetPolicy(hash)` (worker-plane). Full
public-facing proto comments on every one.

**Capture.** `TaskPlugin::network_capture()` (defaulted); `netd` implements
`network_gate()` / `set_network_gate()` for gated allocations as
policy-withheld. OCI bundle gains a network namespace entry with `path`.
`exec` hardening gains `network = "gated"`. `sandbox` gains `net = "gated"`,
vsock through `VmSpec` next to `TapNet`, and the guest agent in the root-disk
build. `daemon.rs` publishes `pool` and `nodeId` into `config_attrs` so
`count.per` can express one-per-node handler jobs.

**Docs.** New `docs/network-handlers.md` (indexed in `docs/README.md` and
root `README.md`) carrying the honesty table verbatim; updates to
`service-discovery.md` (external upstreams, chain on connect),
`task-runtime-sandbox.md`, `task-runtime-container.md`, `task-runtime-exec.md`
(`gated` on each), `edge-boot.md` (the gate on gated allocations),
`metrics.md`, `event-stream.md`, `agent-readiness.md`, `plugin-system.md`
(handler tiers; why `cdylib` is not one), `upgrades.md` (`netd` restart
semantics). `configs/jobs/maximal.ncl` gains every field as each phase lands;
new `configs/jobs/sandbox-egress-demo.ncl` and
`configs/policies/sandbox-egress.ncl`.

---

## 9. Implementation Phases

Egress before ingress: every capability in the original request is an egress
capability, and ingress's marquee scenario needs the CA anyway.

| # | Phase | Ships on its own because… |
| --- | --- | --- |
| **0** | **Policy + datapath core.** `netpolicy` types, merge, validation; `net_policy` table + RPCs + `legion net policy apply/list/show/test`; connect-time rules evaluated in the forwarder ahead of `splice`; external upstreams with `tls = "originate"`; metrics, events, access log; the `netd` interface (in-process for now). | `connect.upstreams` gains policy and its first telemetry; explicit egress to external APIs with an allow-list works on every runtime and platform; posture can be applied to a pool today |
| **1** | **HTTP mode.** HTTP/1.1 framing on chains and external upstreams; path/method/header rules; structured 403s; built-in `dlp` (bounded buffer, `maxBytes`, `onError`). | S2 — body-visible filtering of LLM traffic, no MITM |
| **2** | **`gated` on Linux for `container` and `exec`.** `netd`-owned empty namespaces, loopback proxy + DNS inside, `resolv.conf`, `network_capture()`, the edge-boot gate as policy-withheld, readiness gating, `net_capture_unavailable`. | Unbypassable egress for the two runtimes that need no guest agent; the `boot` gate becomes real for host runtimes |
| **3** | **`legion netd` as a process** with fd handoff, supervision, node health. | Running workloads keep their network across worker and datapath restarts — required before phase 4 makes the datapath a sandbox's only path |
| **4** | **`gated` for `sandbox`.** Guest agent (SOCKS5 + CONNECT + DNS over vsock), no-NIC VM, ingress over vsock. | **The core use case is complete.** Also unblocks microVM `exec` and fork-identity reconfiguration |
| **5** | **Ingress chains.** Port swap, `netd` binds advertised ports, `legion net doctor`, capabilities + MCP. | Anything can be fronted; unblocks wake-on-request scale-to-zero |
| **6** | **External handlers + mTLS.** `authz` webhook with cache; `proxy` by service/port/node with PROXY v2; `net handler test`. mTLS **6a** BYO PKI, **6b** Legion workload CA; kTLS passthrough behind a probe. | Bring-your-own decision or proxy; 6b closes "mTLS between forwarders" |
| **7** | **UDP** (SOCKS5 `UDP ASSOCIATE`, per-datagram rules, flow table) and **transparent capture** (TUN + userspace stack in the gated namespace so tools that ignore `HTTP_PROXY` work). | Completes the protocol matrix; ergonomics, not security |
| **8** | **WASM handlers**, fuel/memory/deadline envelope. | Last, because phase 6 already answers "run my code" out of process. `cdylib` handlers are not planned at all |

---

## 10. Testing Plan

**Unit.** Rule matching (glob hosts, `**` paths, header globs, first match,
fallthrough); `allow` desugaring; submit rejection for path-in-tcp, unknown
handler, h2, dangling `proxy` service; **the merge**: `require` before
`default`, priority order, tie → `apply` error, `require` cannot be weakened,
inline block == task-scoped object; PROXY v2 encoding; the handler-table
no-drift test; content hash stable across field order.

**Datapath.** No-policy chain is a byte-exact passthrough that reaches
`splice`; connect-time deny closes without dialing and without parsing; `dlp`
rejects a body over `maxBytes` per `onError`; each hop's `timeout` bounds the
connection; `authz` cache honors `ttl` and never serves a stale deny as allow.

**Capture — the tests that make the claim.** In a gated container, exec, and
sandbox: a denied host is unreachable by hostname, by literal IP, by raw
socket, and after `unset HTTP_PROXY`; DNS for a denied host returns
`NXDOMAIN` and emits `net_denied`; a query to `8.8.8.8:53` has no route; an
allowed `pip install` succeeds. **If these tests do not exist the word
"unbypassable" comes out of the doc.** Plus: a `boot`-gated allocation with a
gated namespace has no route before ratification and its policy after.

**Lifetime.** Kill and restart the worker: running tasks keep their network,
new connections succeed, `net_capture_unavailable` never fires. Kill `netd`:
tasks stay up, listeners are back within the supervision interval, the
allocation is unhealthy in between and healthy after. Upgrade `netd` with fd
handoff: zero refused connections during the swap.

**Policy delivery.** Apply → live within one heartbeat, no restart; a thousand
allocations with one policy → one `GetNetPolicy` fetch; manager outage → last
known good; explicit empty → authoritative; gated allocation before first
policy → no network, task not started, reason visible.

**Ingress.** Advertised vs private ports; catalog advertises public; deny-all
ingress → failing health check that `doctor` names.

**mTLS.** Wrong CA, expired cert, identity outside `requireIdentity` — each
rejected with the identity in the event; rotation keeps established
connections; kTLS path and userspace fallback produce identical results.

**Regression.** A job with no policy and no capture: byte-identical
`TaskAllocation`, no listener, no store query; `build_upstream_endpoints` fast
path still zero-query; the existing edge-boot gate tests unchanged for
`mode = "tap"`.

**Benchmarks** (§7.12), on the base branch and the change, in the PR, per
`docs/testing-and-benchmarks.md`.

---

## 11. Rollout and Backward Compatibility

- **Nothing changes for existing jobs.** Every field optional; no policy
  matches by default; `capture` defaults preserve today's networking for
  `container` and `exec`.
- **No migrations to existing tables**; one new table.
- **Version skew is a placement constraint.** An older worker would ignore
  `task_net_capture` and run a sandbox with its policy silently dropped — the
  worst outcome this feature can produce. The manager refuses to place a
  captured or policied allocation on a worker whose fingerprint lacks
  `net.policy` support, with a legible reason. Managers first, per
  `docs/upgrades.md`.
- **`netd` introduction (phase 3)** is a worker upgrade; the first start
  after it migrates listeners from in-process to `netd` with a documented
  one-time connection drop.
- **Policy objects are opt-in**; `seedPolicies` is unset by default.
- **A `require` policy applied to a pool with running jobs** takes effect on
  the next heartbeat for every job there. `legion net policy show` is how an
  author sees it coming; `apply --dry-run` prints which running allocations
  would be affected.

---

## 12. Open Questions

1. **Does a `default`-enforcement policy that adds `capture = "gated"` count
   as the task "declaring a chain"?** Lean: capture and chain are independent;
   a task's own `egress` block replaces a `default` policy's `egress`, not its
   `capture`.
2. **Per-allocation vs per-node egress listeners inside the host (non-gated,
   `proxyEnv`) case.** Lean: per-allocation, for attribution; revisit at
   density.
3. **`authz` cache key.** Lean: (identity, destination host:port, method+path
   in HTTP mode), with a hard `ttl` cap of 5m so a revoked allow cannot
   outlive a heartbeat cycle by much.
4. **Should `netd` be optional on macOS**, where nothing is gated and the
   in-process forwarder is fine? Lean: yes — `netd` is Linux-only in phase 3,
   macOS keeps the in-process path behind the same interface.
5. **`dlp` on streamed bodies.** Lean: buffer up to `maxBytes`, never stream a
   regex; over the cap → `onError`. The unbounded case is the one that ate
   Envoy's early ext_proc.
6. **Does the guest agent live in this repo as a workspace crate or ship as
   an artifact?** Lean: workspace crate, static musl, embedded in the binary
   and written into the root disk at build — one version to reason about.
7. **Edge-boot's recommended default gate is still open (its call 18).** If
   `gated` capture makes `boot` a real boundary everywhere, does that change
   the recommendation? Lean: yes, once phase 2 lands — say so in
   `docs/edge-boot.md` then, not now.

---

## 13. Acceptance Criteria

**Phase 0** — `legion net policy apply` with `selector = { nodePools = [...] }`
denies a `connect.upstreams` dial to a non-allow-listed destination without
dialing; the denial names the policy and rule in the event; an external
upstream with `tls = "originate"` serves plaintext-from-task
HTTPS-to-destination; a job with no policy is byte-identical on the wire.

**Phase 1** — `GET /repos/x/y` allowed, `GET /user` denied with a 403 whose
body names the rule; `dlp` rejects a credential-shaped body; a path in a
`tcp` chain rejected at submit with the fix.

**Phase 2** — a gated container cannot reach a denied host by name, IP, raw
socket, or after `unset HTTP_PROXY`; DNS for it returns `NXDOMAIN`; `doctor`
reports capture *enforced*; a `boot`-gated gated container has no route
before ratification.

**Phase 3** — restart the worker under load: running tasks keep their
network, zero refused new connections through `netd` fd handoff.

**Phase 4** — the phase 2 criteria inside a `sandbox`; `pip install` from an
allowed index succeeds; a raw-socket connect to a literal IP has no route.

**Phase 5** — reachable on the advertised port; `resolve` returns it;
deny-all ingress → failing health check named by `doctor`.

**Phase 6** — an unmodified third-party proxy works as a hop via PROXY v2; an
`authz` webhook returning 403 denies and its verdict is cached; a client
outside `requireIdentity` rejected with identity in the event; rotation keeps
connections.

**Every phase** — fmt / clippy / `cargo test --all --locked` green; the public
doc and `internal_docs/TODO.md` updated in the same change; benchmark numbers
in the PR for anything touching the datapath.
