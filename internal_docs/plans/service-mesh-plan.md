# Plan: The Built-in Service Mesh (unified)

> **Judgement calls:** [`calls/service-mesh-plan_calls.md`](../calls/service-mesh-plan_calls.md)

> **Status: SHIPPED (M1–M10), with two named deferrals.** Every phase in §4
> is in `main`'s history on this branch and documented in
> [docs/network-handlers.md](../../docs/network-handlers.md); the honest
> list of what each phase left out is in
> [TODO.md](../TODO.md) under Networking. Deferred, by name: the
> `legion netd` **process split** with `SCM_RIGHTS` hand-off (M5 shipped as
> datapath persistence + replay before the first heartbeat — calls entry
> 17), and flipping `network.mode = "auto"` to `gated` for sandboxes until
> the vsock path has booted on a real VMM in CI (calls entry 16). Still
> planned beyond this plan: UDP, transparent capture, WASM handlers, kTLS.
>
> This is the authoritative plan for everything
> east-west and edge in Legion after service-discovery v1. It **unifies** two
> documents and wins over both where they differ:
>
> - [service-discovery-final-plan.md](service-discovery-final-plan.md) §8, the
>   "mesh follow-on" (DNS + templates, datapath plugin, mTLS, L7 waypoint,
>   WASM, observability). Every item there is re-examined in §2 and either
>   kept, changed, deferred, or dropped — with the reason.
> - [network-handlers-plan.md](network-handlers-plan.md), the ingress/egress
>   policy, capture, and handlers design. It remains the detailed design for
>   those three concepts; this plan changes one thing in it (`capture` becomes
>   a value of `network.mode`, §3.3) and sequences it.
>
> Service-discovery v1 (ports, catalog projection, `service`/`connect`,
> `list`/`resolve`/`doctor`) is shipped and untouched.

---

## 1. What "world-class" means here, and what it does not

A mesh is judged by four things: can I find a service, can I reach it safely,
can I control who reaches mine, and can I see what happened. Legion has to do
all four **without a second system to operate** — no sidecar per task, no
Envoy control plane, no CRDs, no separate CA product — and it has to do them
on a laptop as well as a fleet. Those constraints are the design, not a
limitation of it.

Concretely, when this plan is done a job author can write:

```nickel
service = { port = 5432 },                       # I am findable
connect = { upstreams = [{ service = "db" }] },  # I reach db at db.svc.legion:5432
```

and the operator can write, once, for a pool:

```nickel
{ name = "prod-posture", selector = { nodePools = ["prod"] }, enforcement = "require",
  egress = { policy = "deny", allow = ["*.internal.example.com"] },
  ingress = { tls = { mode = "mutual" } } }
```

and both of them can open the UI and see who talks to whom, what was denied
and by which rule, and — when something cannot connect — run one command that
names the layer that said no.

**Not world-class by any definition, and not attempted:** an xDS server,
Envoy compatibility, multi-cluster federation, an overlay network with
per-allocation IPs, or HTTP/2 routing in v1.

---

## 2. Every mesh decision, re-examined

The service-discovery plan's mesh design predates the handlers plan, workload
identity, edge boot, and a measured L4-vs-L7 cost study. Each item, with the
verdict:

| Item (from SD plan §4–§9, final plan §8) | Verdict | Why |
| --- | --- | --- |
| **Node-local DNS** at `127.0.1.53`, `resolv.conf` injected into every task, `<service>.svc.legion` → A records of healthy endpoints; VIP recommended for LB | **Changed.** DNS is served by `netd`, and `<service>.svc.legion` resolves to the task's **upstream VIP** — never to raw endpoints | An A record cannot carry a dynamic port, so DNS-direct discovery of Legion services was never going to work for the common case; and the critique already made VIP-first the recommendation (B3). Making the VIP the *only* DNS answer deletes the truncation/TC/large-answer machinery (B2) entirely. §3.2 |
| Push endpoint deltas per service over the worker stream, fail-static | **Kept, already shipped** — `HeartbeatReply.upstream_endpoints` | — |
| Template functions `{{ service }}` / `{{ range endpoints }}` with coalesce, splay, fail-static | **Kept**, phase M10 | consul-template parity for apps that need a config file; reuses the KV `on_change` vocabulary |
| **Datapath plugin ABI** + bridge/CNI shim, `mode = "auto"` picking per node | **Deferred.** `network.mode` gains `gated` now; `bridge` / `cni/<name>` remain reserved values | A gated namespace delivers isolation and enforceable policy with no IPAM, no bridge, no `CAP_NET_ADMIN`. Per-allocation routable IPs are a different product for a different buyer; nothing in §1 needs them |
| **Per-node L4 forwarder**, SLO'd, zero-drop reload | **Kept and strengthened** — it becomes `legion netd`, a supervised process with fd handoff | The critique (A3) asked for exactly this; the handlers plan's lifetime finding (tasks survive a worker restart, the forwarder does not) makes it required |
| **mTLS with per-workload certificates** from a workload CA, rotated | **Changed.** A **node CA**: `netd`↔`netd` mTLS on node identity; the sending `netd` attributes the workload (`{region}:{job}:{group}:{task}`) in a PROXY v2 TLV; the receiving `netd` enforces intentions on that attribution. Per-workload certs stay available (`certFrom = "legion"`) for a task's own TLS, later | This is ztunnel's trust model: the node agent is trusted to attribute. It removes the cert-rotation storm the critique flagged (A5) — N node certs, not N×allocations — and it delivers the outstanding "worker mTLS / per-node credentials" item (raft transport auth) with the same CA. §3.5 |
| **L4 intentions** checked by the forwarder against caller identity | **Kept** — enforced at the *receiving* `netd` on the attributed identity; `service.allow` keeps its grammar and gains identity patterns | — |
| **L7 waypoint**: a separate per-service proxy plugin (Envoy/pingora) for retries, timeouts, canary, per-request authz | **Changed.** No waypoint component. `connect.upstreams[].http = { retries, timeout, split, outlier }` is `netd`'s HTTP mode on that upstream | One datapath, not two. L7 cost is still opt-in per upstream, which was the waypoint's whole point; the plugin boundary bought nothing but a second process to operate. §3.4 |
| **L7 intentions** (`allow = [{ l7 = { path, methods } }]`) | **Changed** — expressed as ingress `rules` | One rules grammar, both directions (handlers plan §7.2) |
| **WASM middleware**, then native `cdylib` | **WASM deferred** (last phase); **`cdylib` dropped** | handlers calls entry 4 |
| **Observability**: RED metrics + service graph | **Kept**, phase M8, with the UI | The reason people adopt meshes |
| Deregister-before-SIGTERM (drain) | **Kept**, phase M6 | Small, and the rollout footgun it fixes is real |
| Locality-aware balancing, fail-open | **Kept small** — `balance = "local_first"` in the `BALANCERS` table, widening when local healthy endpoints < 1 | One row in the registry |
| Self-preservation on correlated failure | **Deferred** — a catalog concern, not a mesh one | — |
| Prepared queries, `legion://` gRPC resolver, multi-region | **Deferred** | — |
| DNS-SD PTR / TXT metadata records | **Dropped** | `legion service list` and the UI answer the same question; no demand |

**What the audit did not shake:** catalog-as-projection, per-node-never-per-task,
identity-not-IP, L4-default-L7-opt-in, AP read / CP write, fail-static. The
handlers plan is consistent with all six, which is why it can be the detailed
design for the policy half.

---

## 3. The unified architecture

### 3.1 One datapath: `legion netd`

Everything below is one process per node (in-process until phase M5), one
codebase, one set of metrics:

```
                          ┌────────────── legion netd ──────────────┐
  task ──► VIP / loopback ┤ policy (rules) → handlers → balancer → dial ├──► service endpoint
  task ──► 127.0.0.1:53   ┤ DNS: svc.legion → VIP; external → host,   │      or external host
                          │      policy at resolution                 │      (TLS originated)
  peer ──► advertised port┤ ingress: mTLS terminate → rules → handlers ├──► task private port
                          │ node mTLS ↔ other netd (attributed identity)
                          └──────────────────────────────────────────┘
```

Policy objects, capture, and handlers are exactly as in
[network-handlers-plan.md](network-handlers-plan.md) §7.1–§7.11.

### 3.2 Name-is-config: VIPs and DNS

Today an upstream is `LEGION_UPSTREAM_DB=127.0.0.1:20017` — correct, and
awkward in a config file. After M3 it is also **`db.svc.legion:5432`**:

- Each declared upstream gets a **VIP** in `127.0.0.0/8`, unique per
  allocation on the node (Linux routes the whole /8 to `lo` with no
  configuration). `netd` listens on `<vip>:<canonical port>`.
- The **canonical port** is, in order: the upstream's `localPort`; the
  service's declared in-task port (`network.ports[].to`); the service's
  static port; else a dynamic loopback port (today's behavior).
- `netd` answers DNS for `<service>.svc.legion` with the VIP, and
  `<instance>.<service>.svc.legion` with a VIP bound to one keyed instance
  (the headless analog). `LEGION_UPSTREAM_<SVC>` becomes `<vip>:<port>` so
  nothing that reads it today changes.
- macOS configures only `127.0.0.1` on `lo0`; there, upstreams keep loopback
  ports and DNS answers `127.0.0.1` with the port in an SRV record. Honest,
  documented.
- The resolver reaches tasks: on loopback inside a gated namespace; via the
  `resolv.conf` bind-mount for `container` (it exists — `dns_mounts`); via
  the mount namespace for hardened `exec`; else `LEGION_DNS=<addr>` in the
  env and the operator's resolver config. External names are resolved by the
  host resolver with egress policy applied at resolution (handlers §7.5).

### 3.3 `network.mode` — the datapath selector, finally read

`NetworkConfig.mode` is parsed, stored, and read by nothing. It becomes the
selector the service-discovery plan reserved it for:

| `mode` | Meaning | Default for |
| --- | --- | --- |
| `host` | host networking; policy is cooperative (proxy env) | `container`, `exec` |
| `gated` | the namespace contains loopback and `netd` only — unbypassable | `sandbox` |
| `auto` | `gated` where the runtime and platform can, else `host` | — |
| `bridge`, `cni/<name>` | reserved for the datapath plugin | — |

This replaces the handlers plan's `capture` key. One field, already on the
wire, already persisted.

### 3.4 Traffic management lives on the upstream

```nickel
connect = {
    upstreams = [
        { service = "db" },                                   # L4, as today
        {
            service = "api",
            http = {
                timeout = "3s",
                retries = { attempts = 2, on = ["5xx", "connect-failure"], budget = "20%" },
                split = [{ tag = "canary", weight = 10 }],    # by service tag; the rest to the others
                outlier = { consecutive5xx = 5, ejectFor = "30s" },
            },
        },
        { name = "openai", to = "https://api.openai.com" },  # external, TLS originated
    ],
},
```

Retries default to idempotent methods only, with a budget so a failing
backend cannot be retried into the ground. `split` selects by service tag,
which rollouts already set per version, so a canary is a tag and a weight.
`outlier` feeds the same per-listener endpoint set the balancer reads; an
ejected endpoint is skipped, not deregistered.

### 3.5 Identity on the wire

- **Node CA** (M7): the manager issues node certificates whose SAN is the
  node identity `sub` (`{region}:node:{pool}`), delivered with the node
  identity token, rotated by the same loop with jitter.
- `netd`↔`netd` hops are mTLS on node certs. The sending `netd` prepends a
  PROXY v2 header whose Legion TLVs carry the *workload* identity it is
  forwarding for; the receiving `netd` trusts the node peer to attribute, and
  enforces `service.allow` / `requireIdentity` on the attribution.
- A task that wants its own certificate (to speak TLS itself, or to present
  to a non-Legion peer) uses `certFrom = "legion"`, which issues a workload
  cert with the workload `sub` — later, on demand.
- BYO PKI (files or artifacts) works on ingress and egress from M7a.

### 3.6 Drain before stop

On a rollout, drain, or stop, the allocation is marked draining **before**
`SIGTERM`: the endpoint leaves the heartbeat feed, `netd` stops routing new
connections to it and lets existing ones finish for the drain window, then
the stop ladder runs. The per-node forwarder does not die with the app —
which is the structural advantage over sidecars — so this is an ordering
change, not new machinery.

### 3.7 DX principles the surfaces are held to

1. **One line to declare, one line to consume.** `service = { port = N }` and
   `connect = { upstreams = [{ service = "x" }] }` are complete.
2. **Name-is-config.** `x.svc.legion:port` works in a URL, a config file, a
   library default. No env plumbing required.
3. **Nothing to apply in order.** Policy objects merge deterministically;
   a task's own block is the same object; `legion job plan` shows the merge.
4. **Every denial says why**, to the workload (403 body / `Legion-Denied-By`),
   to the operator (event), and to the agent (`policy test`).
5. **One command names the failing layer.** `legion service doctor` for
   discovery, `legion net doctor` for the datapath; both verdict + cause + fix.
6. **The UI shows the graph, not the config.** Who calls whom, with RED
   numbers and denials, is the first thing on the screen.

### 3.8 UI flows

Each ships in the phase that provides its data.

| Flow | Where | Phase |
| --- | --- | --- |
| **See a service's consumers and callers.** Service detail gains *Consumers* (jobs declaring it as an upstream, with their `allow` verdict) and *Observed callers* (from the graph) with RED | `ServiceDetail` | M1 (consumers), M8 (RED) |
| **See and edit network posture.** A *Network* page: policy objects (selector, enforcement, matched jobs), a *Test* widget (job + destination → verdict + rule), recent denials | new `Network` page | M1 |
| **See what a task actually gets.** Task detail gains a *Network* section: mode and whether enforced, effective egress/ingress with each rule's source policy, upstreams with VIP/port, recent denials | `TaskDetail` | M1, M3 |
| **See the mesh.** The service graph: nodes = services (health %), edges = observed calls with RED and denied-edge highlighting; click an edge for the last denials | `Network` page | M8 |
| **Add middleware and see it in the path.** Task detail shows the handler chain hop by hop with health; `net doctor` output rendered | `TaskDetail` | M7 |
| **Watch a canary.** Service detail shows the split and per-tag RED side by side | `ServiceDetail` | M8 |

---

## 4. Phases

Each ships on its own, with docs, `maximal.ncl`, TODO, and the UI flow it
enables. `H<n>` maps to the handlers plan's phase.

| # | Phase | Enables |
| --- | --- | --- |
| **M1** (H0) | **Policy objects + datapath core.** `netpolicy` types and merge; `net_policy` table, RPCs, `legion net policy apply/list/show/test`; connect-time rules ahead of the dial; external upstreams with TLS origination; metrics, events, access log; the `netd` interface. UI: Network page (policies, test), Task detail network section, Service detail consumers. | Posture per pool today; explicit egress to external APIs with an allow-list on every runtime and platform |
| **M2** (H1 + waypoint) | **HTTP mode + traffic management.** HTTP/1.1 on upstreams and chains; path/method/header rules; structured 403; `dlp`; `http = { timeout, retries, split, outlier }`. | Body-visible filtering without MITM; canaries, retries, timeouts |
| **M3** | **Name-is-config.** `netd` DNS, `svc.legion`, VIPs on 127/8, canonical ports, `resolv.conf` injection where a namespace exists, `LEGION_DNS`. | `db.svc.legion:5432` |
| **M4** (H2) | **`mode = "gated"` on Linux for `container` and `exec`**, edge-boot gate as policy-withheld, readiness gating. | Unbypassable egress for the two runtimes that need no guest agent |
| **M5** (H3) | **`legion netd` as a process**, fd handoff, node health. | Workloads keep their network across worker restarts |
| **M6** (H5) | **Ingress chains + drain-before-stop + `net doctor`.** | Anything can be fronted; rollouts stop refusing connections |
| **M7** (H6) | **External handlers + mTLS.** `authz`, `proxy`; BYO PKI; **node CA** and `netd`↔`netd` mTLS with attributed identity; `local_first`. UI: handler chain in Task detail. | Cryptographic intentions; bring-your-own decision or proxy |
| **M8** | **Service graph.** RED per (caller, service) aggregated on the heartbeat; `ListServiceEdges`; UI graph, per-tag RED, denied edges. | The reason to adopt a mesh |
| **M9** (H4) | **`gated` for `sandbox`** over vsock with the guest agent. | The sandbox story complete |
| **M10** | **Templates** (`{{ service }}`, `{{ range endpoints }}`, coalesce/splay/fail-static). | Config-file apps |
| deferred | UDP, transparent capture (TUN), WASM, datapath plugin, prepared queries, federation, workload certs on demand. | — |

---

## 5. Testing, rollout, acceptance

Testing and rollout for policy, capture, handlers, `netd`, and ingress are
the handlers plan's §10–§11 and are not restated. Added by this plan:

- **DNS/VIP.** `db.svc.legion` resolves to the VIP inside a gated namespace
  and inside a container; the VIP serves the canonical port; two allocations
  on one node get distinct VIPs; macOS falls back to SRV + loopback port.
- **Traffic management.** A 503 from one of two endpoints is retried on the
  other within budget; a POST is not retried; `split` sends ~10% to the
  `canary` tag over 1,000 requests; five consecutive 5xx eject an endpoint
  for `ejectFor` and it returns after.
- **Node mTLS.** A `netd` with a cert from the wrong CA is refused; the
  attributed identity is what the receiving side's `net_denied` event shows;
  rotation does not drop connections.
- **Drain.** During a rollout, a client dialing the service through the
  forwarder sees zero connection-refused errors.
- **Graph.** Edges appear within one heartbeat of the first call and carry
  the right verdict split.
- **Rollout.** `network.mode` absent means `host` for container/exec and
  `gated` for sandbox — sandbox already had no network by default, so the
  only change a sandbox author sees is that declared upstreams now work.

Acceptance per phase is observable and listed in the handlers plan for the
shared phases; for M2/M3/M7/M8 it is the bullet list above.
