# Service Discovery: Self-Critique & Revisions

> A deliberate red-team of [service-discovery-plan.md](service-discovery-plan.md) and
> [service-discovery-mesh-and-config.md](service-discovery-mesh-and-config.md), informed
> by a survey of concrete failure modes in K8s, Nomad, Consul, Istio/Envoy, Eureka, ZK,
> SWIM/Serf, Cilium, Fly, Tailscale, and others. Every weakness below is something the
> *current* plan gets wrong, hand-waves, or leaves exposed. Each ends with a concrete
> revision. The highest-severity revisions (★) are folded back into the main plan.

---

## Part A — Structural critique (the uncomfortable, plan-level problems)

These aren't bugs in a detail; they're things wrong with the shape of the whole plan.

### A1. ★ The plan never states its CAP posture — and the default is probably wrong

The plan says "the catalog is a projection over allocation + check state committed to
SQLite." But Legion's store can be **Raft (CP)** or single-writer litestream. As written,
discovery *reads* implicitly go to the catalog, so **if the manager/Raft leader is
partitioned, discovery could stop** — exactly the failure that makes CP registries a bad
fit for discovery. The entire survey converges on the opposite: discovery wants **AP**
(Eureka self-preservation, Serf gossip, Fly's per-host materialized DB, Tailscale's
pushed map). I gestured at "push to node-local resolvers" but never made the guarantee
explicit, and a reviewer would (correctly) assume lookups fail during a manager outage.

**Revision (folded in):** State the posture explicitly and per-layer. **The catalog
*write* path is CP** (it's a projection of Raft-committed scheduler truth — we *want*
consistency there). **The discovery *read* path is AP**: each node's resolver/forwarder
serves its **last-known-good** endpoint set from local state and keeps serving through a
manager partition. New registrations may stall during a partition; existing discovery
must not. This is the Eureka/Fly/Tailscale "materialize locally, survive the CP outage"
principle, now a stated invariant.

### A2. The plan is too big to ship — simplicity tenet vs. 7-phase sprawl

I wrote a tenet called "simple but flexible," then specified ports + catalog + DNS +
push + datapath plugin ABI + CNI shim + L4 forwarder + mTLS + intentions + L7 waypoint +
WASM middleware + observability. That's a multi-quarter program. The real risk — the one
that killed App Mesh and SMI — is **shipping so much mesh that the simple core never
lands or never gets adopted**. The plan doesn't draw a hard line around "what is v1."

**Revision:** Declare a hard v1 boundary: **v1 = Phases 1–3 only** (ports + env vars,
the catalog projection, DNS + templates). That alone closes every box in the TASK-LIST
except intentions, and it has **no new data-plane component**. The L4 forwarder, mTLS,
intentions, L7, and middleware are explicitly **post-v1**, each independently justified.
"Discoverable and reachable by DNS/env/template" is a complete, useful product; the mesh
is an opt-in second product. (See revised phasing, Part E.)

### A3. The per-node L4 forwarder is a large new data-plane component I waved at

"A per-node userspace forwarder, like ztunnel" is one sentence hiding a serious build: it
sits on **every connection**, is a per-node SPOF, adds a hop, and must handle
connection draining, backpressure, and its own upgrades without dropping traffic
(SmartStack's HAProxy-reload blips are the cautionary tale). On macOS it's *mandatory*
(no eBPF acceleration), so the slow path is the only path there.

**Revision:** Treat the forwarder as its own mini-plan with explicit SLOs (added p99
latency budget, max connections, zero-drop reload via FD hand-off / SO_REUSEPORT), and
make **DNS-direct + client-side retry a first-class fallback** so the forwarder is never
the *only* way to reach a service. Acknowledge it as the riskiest single component and
gate it behind v2.

### A4. "Works on Mac" is shakier than I implied

I claimed full discovery on macOS. Real gotchas the plan ignores: macOS **does not honor
`/etc/resolv.conf`** the way Linux does for all lookups — you need a scoped resolver
under `/etc/resolver/<zone>` or a real listener, and per-process DNS for `exec` tasks is
awkward; **host-mode port collisions** (my own Open Question 2) are guaranteed without
namespaces; and there's no isolation, so intentions are advisory unless the forwarder
mediates *all* traffic (which on a shared host net it can't fully guarantee).

**Revision:** Downgrade the claim to exactly what's true: on macOS you get **catalog +
DNS-via-scoped-resolver + env vars + (when used) the forwarder's localhost upstreams**;
you do **not** get isolation or enforceable L4 intentions without routing through the
forwarder. Document the `/etc/resolver/legion` mechanism specifically. "Mostly on Mac" —
honestly scoped.

### A5. mTLS-everywhere inherits the cert-rotation-storm problem and a hard dependency

Intentions + forwarder mTLS depend on the [WI/OIDC plan](workload-identity-oidc-plan.md)
landing first, and "short-lived auto-renewed certs" at fleet scale is precisely the
**cert-rotation storm** Istio is criticized for. The plan doesn't sequence the dependency
or address rotation load.

**Revision:** Make mTLS/intentions strictly *depend on* WI landing, stagger renewals with
jitter (no synchronized expiry), and allow a non-mTLS L4 mode for clusters that haven't
adopted WI. Don't let the mesh block the discovery core.

---

## Part B — Failure-mode audit (researched pitfall → are we exposed? → fix)

| # | Researched failure mode | Exposed in current plan? | Mitigation (★ = folded into plan) |
|---|---|---|---|
| B1 | **conntrack 5s DNS race** (UDP DNAT/SNAT race, [k8s#56903](https://github.com/kubernetes/kubernetes/issues/56903)) | **Yes, latent.** Node resolver at `127.0.1.53` is reached by a bridged task *across* the veth/NAT → same UDP-over-NAT race. | ★ Resolver must be reachable **without host masquerade**: bind it on the bridge gateway / inside the task netns so task→resolver has **no SNAT/DNAT**. Resolver↔manager push uses **TCP** (or a persistent stream), never racy UDP. Recommend single-socket TCP for large answers. |
| B2 | **Headless DNS truncation** (~88 A records/UDP; Go pure resolver fails on TC bit; musl <1.2.4 no TCP fallback) | **Yes.** A7 headless "returns all instance records" with no bound. | ★ Bound UDP answers; **set TC and support TCP fallback** correctly; for large sets return a **capped, randomized subset** over UDP and the full set only over TCP/`ResolveService` API. Steer big-fanout consumers to the **forwarder VIP**, not raw record sets. |
| B3 | **DNS LB imbalance** (clients cache one A record) | **Yes.** DNS-direct gives no real balancing. | ★ Make the **forwarder VIP the recommended path** (real, health-aware LB); DNS multi-record is the fallback, served with **randomized order + low TTL**. Document the tradeoff. |
| B4 | **Readiness vs endpoint-withdrawal race** (connection-refused during rollouts, [learnkube](https://learnkube.com/graceful-shutdown)) | **Yes — significant.** Plan removes an endpoint "on health failure" but never **deregisters before stop**. | ★ **Deregister-before-SIGTERM**: on rollout/drain, withdraw the endpoint and let the forwarder/resolver converge (a drain window) **before** stopping the task. Because our forwarder is **per-node, not per-task**, it does *not* die with the app (this structurally beats the "proxy died first" sidecar bug) — but we must order: stop new conns → drain → SIGTERM. |
| B5 | **consul-template render storm / thundering herd** + **task-fails-on-render-failure** ([nomad#13019](https://github.com/hashicorp/nomad/issues/13019)) | **Yes — two ways.** `on_change: restart/rollout` can restart **all** consumers of a popular service at once; and coupling task health to render success means a discovery blip fails tasks. | ★ **Coalesce + splay**: debounce endpoint deltas and apply a randomized `splay` before `on_change` actions; cap concurrent restarts. ★ **Fail static, not fail task**: on render failure keep the **last-good** rendering and mark degraded — never kill the task because discovery hiccuped. |
| B6 | **Topology-aware routing reliability sacrifice** (zone pinning to dead endpoints, [Buoyant](https://www.buoyant.io/blog/the-trouble-with-topology-aware-routing-sacrificing-reliability-to-avoid-cross-zone-traffic)) | **Partially.** Part C locality says "fail over on empty/unhealthy" but doesn't make fail-open the hard invariant. | ★ **Locality is a preference, never a partition.** Invariant: if local-zone healthy endpoints < threshold, **immediately widen to region/global** — never serve zero/dead local endpoints to avoid cross-zone traffic. Explicitly the opposite of K8s TAR's absolute isolation. |
| B7 | **SWIM/observer false positives** (a CPU-starved node falsely fails *others*; [Lifeguard](https://www.hashicorp.com/en/blog/making-gossip-more-robust-with-lifeguard)) + **Eureka mass-eviction** | **Yes.** If a worker self-reports check health and is starved, it yanks its own endpoints; if the manager sees a network blip, it could mass-deregister a whole node/zone. | ★ **Self-preservation + self-awareness**: if a large fraction of a node's/zone's endpoints fail health *simultaneously*, suspect the **observer/network**, not mass death — **stop mass-eviction**, hold last-good, alarm. Borrow Lifeguard's "a degraded detector distrusts its own judgment." |
| B8 | **Liveness zombies** (dead worker leaves stale endpoints) | **Partially.** Projection helps, but a worker dying without deregister could linger until checks time out. | ★ **Liveness-as-lease** (ZK ephemeral-node / etcd-lease model): a node's endpoints are tied to its **heartbeat lease**; lease expiry auto-removes them — no separate reaper, no zombies. Legion already has worker heartbeats to hang this on. |
| B9 | **xDS NACK-all** (one bad field rejects *all* resources, [GCP proxyless limits](https://docs.cloud.google.com/service-mesh/docs/service-routing/limitations-proxyless)) | **Yes, latent.** A malformed service/datapath config in a push could poison the whole node update. | ★ **Per-resource validation + partial apply**: a bad single service entry is dropped and alarmed; it must never blind a node to *all* services. Prefer incremental/delta pushes. |
| B10 | **Env-var E2BIG** (injecting all services' vars → pods won't start, [k8s#92615](https://github.com/kubernetes/kubernetes/issues/92615)) | **No — and good.** We inject only the task's **own** port vars (`LEGION_*`), never all-services. | Keep it that way; **state the non-goal explicitly** so no one "helpfully" adds cluster-wide service env injection. |
| B11 | **Istio CRD/operational sprawl, App Mesh death, Consul BSL** | **No — supportive.** Single binary, no second system, no Consul dependency. | This validates Tenet 1. Note App Mesh's collapse-into-orchestrator as the precedent and that we carry **no BSL discovery dependency**. |
| B12 | **kube-proxy O(n) iptables blowup** | **Mostly avoided.** Forwarder uses map-based endpoint sets, not O(n) rules. | Keep the fast path map/eBPF-based; never generate per-service linear rules. Already a tenet; restate as a hard constraint. |

---

## Part C — Concrete design revisions (the actionable list)

Folded into the plan where marked ★ in Part B. Consolidated:

1. **AP read / CP write**, with node-local last-known-good serving through partitions (A1).
2. **Liveness-as-lease** on the worker heartbeat; lease expiry auto-evicts endpoints (B8).
3. **Deregister-before-stop** drain ordering on rollout/drain (B4).
4. **Coalesce + splay + cap** on endpoint-change `on_change` actions; **fail-static** (keep
   last-good) on template render failure, never fail the task (B5).
5. **Fail-open locality** as an invariant; locality is preference, never isolation (B6).
6. **Self-preservation**: suspend mass-eviction on correlated failure; distrust a degraded
   observer (B7).
7. **No-NAT resolver path + TCP push/answer**; correct truncation + TCP fallback; bounded,
   randomized large answers (B1, B2).
8. **VIP-first, DNS-direct fallback** for load balancing (B3).
9. **Per-resource validation, partial apply, delta pushes** (B9).
10. **Hard v1 line** at Phases 1–3; mesh strictly post-v1 (A2). Forwarder gets its own
    SLO'd sub-plan (A3). mTLS depends on WI + jittered rotation + optional non-mTLS L4
    mode (A5). Honest macOS scope with `/etc/resolver/legion` (A4).

---

## Part D — Good ideas the plan is *missing* (steal list)

Ranked by value-for-effort.

1. **★ Liveness-as-lease** (ZK ephemeral nodes / etcd leases). Cleanest health model in the
   survey: presence == a live lease; no zombie reaper. Already folded (B8).
2. **★ Self-preservation / Local-Health-Multiplier** (Eureka, Lifeguard). A failure detector
   that distrusts itself under correlated failure. Folded (B7).
3. **Prepared-query-style named failover** (Consul). A server-side **named query with an
   embedded failover policy**, addressable as `<name>.query.legion`, so dumb DNS clients
   get **RTT-aware geo-failover** without client changes — and the policy changes centrally,
   not per-client. Even pre-federation, design the *hook*. RTT ordering via node coordinates.
4. **DNS-SD record convention** (RFC 6763 PTR/SRV/TXT) + **Fly TXT metadata**. Add:
   `_services.legion` PTR enumeration of all service types, and `meta.<svc>.svc.legion` TXT
   for `service.meta` — a zero-infrastructure metadata/enumeration API over plain DNS.
5. **Encode topology into the address** (Fly 6PN, Innernet CIDR). If/when we own an overlay
   address plan, bake region/node/instance into the IP so routing + ACL become local
   arithmetic instead of lookups.
6. **`legion://` gRPC resolver** (gRPC name-resolution API). For gRPC apps, ship a resolver
   that returns **endpoints + LB/routing config** (the proxyless path) so they skip the
   forwarder hop entirely — one registry, an extra consumption mode. Mind the **NACK-all**
   lesson (B9): per-resource, not all-or-nothing.
7. **Zone-ingress single hop** (Kuma). For future cross-region, route inter-region traffic
   through *one* ingress proxy per region rather than full mesh reachability — drastically
   simpler firewalling. Pairs with #3.
8. **NATS `$SRV`-style introspection.** Our `ResolveService` RPC already covers enumerate;
   add cheap **PING/RTT + per-endpoint STATS** so the mesh is self-describing (feeds the
   observability service-graph for free).
9. **Cilium Global Services** as the multi-cluster failover model to emulate if/when we do
   federation (health-aware cross-cluster LB in the data path).
10. **`consul-template`-as-a-pattern is already ours** (the template path) — but adopt its
    `splay` and reload-vs-restart discipline (folded in B5).

---

## Part E — Revised phasing (with the hard v1 line)

```
── v1 (the actual product; no new data-plane component) ────────────────
Phase 1  Ports + env-var discovery
Phase 2  Catalog projection + liveness-as-lease + self-preservation
Phase 3  DNS (no-NAT path, TCP fallback, PTR/SRV/TXT, fail-open locality)
         + templates (coalesce/splay/fail-static)
         ── ships: declare a service, find it by env/DNS/template, only-healthy ──
── v2 (the mesh; each independently justified, opt-in) ─────────────────
Phase 4  Datapath plugin ABI + bridge/CNI shim
Phase 5  L4 forwarder (SLO'd, zero-drop reload) + VIP + mTLS (needs WI) + L4 intentions
Phase 6  L7 waypoint (retries/timeout/split) + fail-open locality at L7
Phase 7  WASM middleware (fuel/memory/fail-policy) → 7b native cdylib (operator-only)
── later ───────────────────────────────────────────────────────────────
Phase 8  Prepared-query failover (`*.query.legion`) + multi-region zone-ingress
Phase 9  `legion://` gRPC proxyless resolver
```

The whole TASK-LIST is satisfied by **v1 + Phase 5's intentions**. Everything else is a
deliberate, separately-adopted capability — which is the simplicity the plan promised but
the first draft quietly violated.

---

## Part F — What the critique did *not* shake

For honesty, the parts that survived review intact:

- **Catalog-as-projection** (no drift, no second system) — reinforced by App Mesh/Consul-BSL.
- **DNS-native + `ndots:1`** — directly fixes the K8s ndots tax; the survey only *added*
  the no-NAT and truncation refinements, didn't undermine it.
- **L4-default / L7-opt-in / no per-task sidecar** — reinforced by every mesh-overhead and
  "proxy died first" finding; our per-node forwarder structurally dodges the sidecar bugs.
- **Identity-not-IP intentions** — reinforced by Cilium/Nebula/Tailscale.
- **Datapath plugin as a modern, declarative CNI + CNI shim** — reinforced by netavark/Cilium.
- **WASM-sandboxed middleware** — the right and only safe shape for "arbitrary code on the
  path," confirmed by the proxy-wasm ecosystem.
