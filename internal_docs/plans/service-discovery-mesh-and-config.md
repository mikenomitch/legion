# Service Discovery: Config Cookbook, Observability & Mesh Extensions

> **Judgement calls:** [`calls/service-discovery-final-plan_calls.md`](../calls/service-discovery-final-plan_calls.md)

> Companion to [service-discovery-plan.md](service-discovery-plan.md). That doc owns the
> core model; this one shows **how you configure it across real scenarios**, plans
> **observability** for the mesh, sketches **additional mesh features** (traffic
> management, resilience), and answers a sharp question: *can the mesh run arbitrary
> user code between services, and what would that look like?*

---

## Part A — Configuration Cookbook

All examples are Jsonnet job specs. The guiding principle is **progressive
disclosure**: the first example is the whole API most users need; each later example
adds exactly one concept.

### A1. The absolute minimum — discoverable in one line

```jsonnet
{
  name: 'redis', kind: 'container',
  config: { image: 'redis:7' },
  service: { port: 6379 },           // bare int declares the port inline; no `network` block.
                                      // name defaults to task name -> redis.svc.legion
}
```

Result: `redis.svc.legion` resolves to this instance; `LEGION_ADDR_redis` is set in the
task; it's in the catalog and the UI. No checks yet → it registers as soon as the alloc
is running. (An integer/`'dynamic'` `service.port` desugars to a `network.ports` entry —
you only write an explicit `network` block when a task has multiple ports, non-service
ports, or custom host↔container mapping; see A4/A6 and plan §3.)

### A2. Add only-healthy semantics (reuse existing `checks`)

```jsonnet
{
  name: 'redis', kind: 'container',
  config: { image: 'redis:7' },
  service: { port: 6379 },           // inline port; checks probe the same port number
  checks: [
    { type: 'readiness', kind: 'tcp', port: 6379, every: '5s', timeout: '2s' },
    { type: 'health',    kind: 'tcp', port: 6379, every: '10s', timeout: '2s',
      unhealthy_after_count: 3 },
  ],
}
```

Now `redis.svc.legion` returns the instance only after readiness passes, and drops it on
the 3rd consecutive health failure.

### A3. Consume a service — the three ways, side by side

```jsonnet
{
  name: 'web', kind: 'container', count: 3,
  config: { image: 'mycorp/web' },
  network: { mode: 'auto', ports: [{ name: 'http', to: 8080 }] },

  // (1) ENV VAR — its own port, zero-dependency:
  //     app reads LEGION_ADDR_http at startup.

  // (2) DNS — fully dynamic, no SDK:
  //     app dials redis.svc.legion:6379 directly.
  env: [{ name: 'REDIS_HOST', value: 'redis.svc.legion' }],

  // (3) TEMPLATE — render a config file, react to endpoint changes:
  files: [{
    path: '/etc/web/redis.conf',
    file: { template: '{{ with service "redis" }}{{ (index . 0).Addr }}:{{ (index . 0).Port }}{{ end }}' },
    service: { on_change: 'signal', signal: 'SIGHUP' },   // reload without restart
  }],

  service: { name: 'web', port: 'http' },
  checks: [{ type: 'readiness', kind: 'http', path: '/ready', port: 8080, every: '5s' }],
}
```

### A4. Transparent upstreams — dial `localhost`, mesh routes it

```jsonnet
{
  name: 'web', kind: 'container',
  network: { mode: 'auto', ports: [{ name: 'http', to: 8080 }] },
  service: { name: 'web', port: 'http' },
  connect: {
    // app connects to 127.0.0.1:6379; the per-node L4 forwarder routes to a
    // healthy `redis` over mTLS. No code change to talk to a remote service.
    upstreams: [{ service: 'redis', local_port: 6379 }],
  },
}
```

### A5. Access control by identity (intentions)

```jsonnet
{
  name: 'redis', kind: 'container',
  network: { ports: [{ name: 'redis', static: 6379 }] },
  service: {
    port: 'redis',
    allow: [
      { job: 'web' },                       // any task in job `web`
      { job: 'admin', task: 'cli' },         // one task in job `admin`
    ],
    // everything else is refused at connect-time over mTLS identity.
  },
}
```

### A6. Region-scoped and tagged discovery

```jsonnet
{
  name: 'api', kind: 'container', count: { num: 6, per: 'region' },
  network: { mode: 'auto', ports: [{ name: 'grpc', to: 9000 }] },
  service: {
    name: 'api', port: 'grpc',
    tags: ['v2', 'grpc'],
    meta: { protocol: 'grpc' },
  },
  checks: [{ type: 'health', kind: 'grpc', port: 9000, every: '10s' }],
}
```

Consumers can resolve:
- `api.svc.legion` — all healthy instances, all regions.
- `us-east.api.svc.legion` — only us-east instances (locality).
- `v2.api.svc.legion` — only instances tagged `v2`.
- `_grpc._tcp.api.svc.legion` — SRV with port + target.

### A7. Stateful / headless — address a *specific* instance

Builds on the [instance-identity plan](stateful-workloads-and-instance-identity-plan.md)
slot mode. Each replica gets a stable per-instance DNS name — the StatefulSet headless
analog (think a Postgres primary at slot 0):

```jsonnet
{
  name: 'pg', kind: 'container', count: { num: 3 },
  stateful: { mode: 'slot', storage: { path: '/var/lib/pg' } },
  network: { ports: [{ name: 'pg', static: 5432 }] },
  service: { port: 'pg', headless: true },   // per-instance records, no VIP
}
```

Resolves: `0.pg.svc.legion` (always slot 0's current placement), `1.pg.svc.legion`, … —
stable across reschedules because the instance id is stable.

### A8. L7 mesh — retries, timeout, canary split (opt-in)

```jsonnet
{
  name: 'checkout', kind: 'container',
  network: { mode: 'auto', ports: [{ name: 'http', to: 8080 }] },
  service: { name: 'checkout', port: 'http' },
  connect: {
    upstreams: [{ service: 'payments', local_port: 7000 }],
    l7: {
      protocol: 'http',
      to: 'payments',
      retries: { attempts: 2, on: ['5xx', 'reset'] },
      timeout: '3s',
      split: [                              // canary: 10% to v2
        { tag: 'v1', weight: 90 },
        { tag: 'v2', weight: 10 },
      ],
    },
  },
}
```

### A9. Choosing a datapath explicitly (override `auto`)

```jsonnet
// Force Linux bridge isolation with a specific CNI network:
network: { mode: 'cni/dmz', ports: [{ name: 'http', to: 8080 }] }

// Force host networking (the macOS default; also useful for host-perf workloads):
network: { mode: 'host', ports: [{ name: 'http', static: 8080 }] }
```

### A10. Cluster-level posture (manager config, not jobspec)

```jsonnet
// configs/manager.jsonnet
discovery: {
  dns: { zone: 'svc.legion', listen: '127.0.1.53', ndots: 1 },
  port_range: { min: 20000, max: 32000 },
  intentions: { default: 'deny' },   // zero-trust: services must opt callers in
  mtls: { enabled: true },           // forwarder originates/terminates mTLS
}
```

> ★ Insight ─────────────────────────────────────
> Notice the **single knob that scales with you**: `network: { ports: [...] }` +
> `service: { port: ... }` is the floor, and *every* advanced capability —
> intentions, L7, canary, headless, locality — is an additive block you never see
> until you ask for it. There is no "now rewrite everything as a mesh" cliff like the
> Ingress→Gateway or native-Nomad→Consul-Connect migrations. The maximal spec and the
> one-liner share the same grammar.
> ─────────────────────────────────────────────────

---

## Part B — Observability Plan for the Mesh

Legion is already OTEL-native (tracing/metrics/logs) per
[docs/observability.md](../../docs/observability.md). Service discovery and the mesh are a
**goldmine of golden-signal telemetry that the app gets for free**, because traffic
already flows through a Legion-owned data path (the node forwarder / L7 waypoint). This
is the same reason meshes (Istio/Linkerd) are adopted primarily *for observability*, not
routing — we get it without a sidecar.

### B1. What we emit, and from where

| Signal | Source | Examples |
| --- | --- | --- |
| **Catalog metrics** | manager | `legion_service_endpoints{service,state}` (healthy/total), registration/deregistration rate, churn per service. |
| **DNS metrics** | node resolver | query QPS, NXDOMAIN rate, cache hit ratio, push-delta apply latency (freshness lag vs manager). |
| **L4 forwarder (RED)** | node forwarder | per `{src_identity → service}`: connections, bytes, active conns, connect errors, mTLS handshake failures, intention denials. |
| **L7 waypoint (RED)** | waypoint plugin | per route: request rate, error rate (by class), p50/p90/p99 latency, retries, timeouts, circuit-breaker trips, per-split-version counts. |
| **Health** | existing checks | check pass/fail transitions already feed the catalog; surface as `legion_check_state`. |

All keyed by **identity** (`region:job:group:task`), not IP — so dashboards survive
reschedules.

### B2. Distributed tracing (the high-value bit)

- The node forwarder / waypoint **propagates W3C `traceparent`** and, when it
  originates a span, emits a mesh span (`legion.mesh.call`, peer = upstream identity).
  This gives a **service dependency graph for free** even for apps that aren't
  instrumented — the single most-requested mesh feature.
- Spans carry `legion.service`, `legion.upstream`, `legion.intention.decision`,
  `legion.split.version` so a trace shows *which canary version* served a request and
  *why* a call was allowed/denied.
- L4-only traffic can't be traced per-request (no HTTP framing) but emits **connection
  spans** (open/close, bytes, identity pair) — still enough for an L4 topology.

### B3. Topology / service graph

The catalog + per-call peer identity yields a live **service map**: nodes = services
(with health %), edges = observed `caller → callee` with RED stats. This is a UI view
([docs/ui-architecture.md](../../docs/ui-architecture.md)) built entirely from data the mesh
already produces — comparable to Kiali/Hubble but with **no extra component to deploy**.
It also flags **intention violations** (attempted-but-denied edges) — a security signal
Hubble is prized for.

### B4. How it fits the existing pipeline

No new telemetry transport: the forwarder/waypoint are Legion processes, so they use the
existing OTEL exporters and the `#[instrument]` conventions in
[AGENTS.md](../../AGENTS.md). The mesh is just more spans/metrics on the same pipe.
Per the differentiators "Day 1 in-core / Day N split" pattern, the metrics scrape and
the service-graph builder can later move into their own job.

---

## Part C — Other Mesh Features (and how each fits)

Each feature is an **additive block on `connect`/`service`**, enforced by the data path
we already have (L4 forwarder for connection-level, L7 waypoint for request-level). None
of them are mandatory; an L4-only service pays for none of them.

| Feature | Layer | Config surface | Fits via |
| --- | --- | --- | --- |
| **Retries / timeouts** | L7 | `connect.l7.{retries,timeout}` | waypoint plugin (A8). |
| **Circuit breaking / outlier ejection** | L7 | `connect.l7.outlier { consecutive_5xx, eject_for }` | waypoint sheds the endpoint, feeds catalog health. |
| **Traffic split / canary / blue-green** | L7 | `connect.l7.split[]` by tag/version | waypoint; ties to rollout strategy plugins. |
| **Fault injection** (delay/abort) | L7 | `connect.l7.fault { delay, abort_pct }` | waypoint; testing only, gated. |
| **Rate limiting** | L4/L7 | `service.rate_limit { rps, burst }` | forwarder (per-conn) or waypoint (per-route). |
| **Locality-aware routing + failover** | L4/L7 | `connect.locality { prefer: 'zone', failover: 'region' }` | resolver returns local-first endpoints, fails over on empty/unhealthy — Consul prepared-query analog, fixing K8s Topology-Hints' "no failover when local zone is empty" gap. |
| **mTLS / identity** | L4 | `discovery.mtls`, `service.allow` | forwarder + WI certs (already in the plan). |
| **Access logging** | L4/L7 | `connect.l7.access_log` | waypoint → OTEL logs. |

**Rollout integration:** canary `split` weights are exactly what the existing
**rollout strategy plugins** ([docs/rollouts.md](../../docs/rollouts.md)) want to drive — a
progressive rollout can shift `split` 10→25→50→100 and watch the mesh's RED metrics
(B1) to auto-rollback on error-rate breach. The mesh closes the loop the rollout system
is missing today: *metric-aware* progression.

---

## Part D — "Arbitrary code between services" (the middleware plugin)

You flagged this as *probably a bad idea*. You're right to be wary — and there's a
**safe shape** for it. Here's the honest design, the dangers, and how to defang them.

### D1. What you're really asking for

A hook to run custom logic **on the synchronous request/response path between two
services** — i.e. mesh middleware. This is exactly Envoy's HTTP filter chain and the
**proxy-wasm** ecosystem (Istio WasmPlugin, Kong, APISIX, Cloudflare Workers-on-the-edge
are the same idea). Use cases: custom auth/JWT validation, header mutation, request
signing, redaction/PII scrubbing, A/B bucketing, custom routing keys, audit, schema
validation.

### D2. The naive (dangerous) version — and why

Legion *already* has in-process `cdylib` plugins. The naive move is "let users write a
`middleware` cdylib and load it into the forwarder." **Do not make this the user-facing
story**, because the data path is the worst place for untrusted/arbitrary code:

- **It's synchronous and hot.** Every request waits on your code. A 5ms `println!`-debug
  or a lock contention becomes p99 latency for the whole service.
- **A panic/segfault takes down the node forwarder** — i.e. *all* mesh traffic on that
  node, not one task (the documented cdylib risk in [plugin-system.md](../../docs/plugin-system.md)).
- **No resource bound.** An infinite loop or 4GB allocation in a native plugin wedges
  the data plane. Head-of-line blocking, OOM.
- **Security.** Native code in the forwarder sees *all* services' traffic and runs with
  the forwarder's privileges. Arbitrary native code there is a cross-service data-exfil
  primitive.
- **Versioning/ABI.** Native ABI drift breaks the data plane on upgrade.

So: native cdylib middleware is acceptable **only for operator-authored, trusted,
reviewed** filters. It is not the answer for "arbitrary code."

### D3. The safe version — sandboxed WASM filters (proxy-wasm-style)

The industry already solved this: run the arbitrary code as **WebAssembly in a sandbox**
inside the L7 waypoint, with a narrow ABI and hard resource limits. This is what makes
"arbitrary code between services" defensible:

```jsonnet
connect: {
  l7: {
    protocol: 'http',
    to: 'payments',
    middleware: [
      {
        name: 'jwt-check',
        wasm: 'oci://registry.legion/mw/jwt-check:1.2',  // pulled like any artifact
        on: ['request'],            // request | response | both
        config: { issuer: 'https://auth.example.com' },
        // safety envelope (all defaulted, all enforced by the host):
        timeout: '2ms',             // epoch/fuel-bounded; over → fail policy
        memory_max: '16Mi',
        on_failure: 'deny',         // deny | allow (fail-closed vs fail-open) | continue
      },
    ],
  },
}
```

The host (waypoint) ABI exposed to the WASM module is deliberately tiny — read/modify
headers, read a bounded body chunk, set a verdict (`continue` / `pause` / `respond
<status>`), emit a metric/log, and **call back into Legion** (resolve a service, read a
KV value, fetch a WI token) through capability-scoped host functions. No raw sockets, no
filesystem, no ambient network — capabilities are granted explicitly.

**Why WASM specifically tames every danger in D2:**

| Danger | WASM mitigation |
| --- | --- |
| Hot-path latency | Per-call **fuel/epoch deadline** (`timeout: '2ms'`); exceed → `on_failure` policy, never an unbounded stall. |
| Crash takes down forwarder | Module faults are **trapped**; the trap fails that one request per `on_failure`, the host survives. |
| Runaway memory/CPU | Linear-memory **cap** + fuel metering; can't OOM or spin the node. |
| Cross-service data access | **Sandbox**: sees only the request it's attached to; capabilities are explicit. |
| ABI drift | Stable proxy-wasm-like ABI; module is version-pinned in the spec, pulled as an artifact. |

### D4. Where it runs, and where it must NOT

- **Runs in the L7 waypoint only** (opt-in, per-route). L7 already has request framing
  and is already opt-in cost — middleware rides that, so L4-only services are untouched
  and pay zero.
- **Never in the L4 forwarder by default.** L4 has no request boundaries; the only
  defensible L4 hook is a **connection-level WASM authz** verdict (allow/deny on
  identity+SNI), with the same fuel/fail-policy envelope — and even that is advanced.
- **Attachment scope is explicit:** per upstream route (above), or cluster-wide as a
  manager-config filter chain for org-wide policy (e.g. mandatory audit logging).

### D5. Two tiers, stated honestly

| Tier | Who writes it | Sandbox | Use |
| --- | --- | --- | --- |
| **WASM middleware** (recommended for "arbitrary code") | anyone | yes — fuel/memory/cap-scoped | untrusted/app-team logic on the path |
| **Native cdylib middleware** | operators only, reviewed | no | trusted, perf-critical org filters |

This mirrors the plan's overall plugin philosophy (in-process native for trusted speed)
but **adds the sandbox tier precisely because "arbitrary code on the data path" demands
it.** It also composes with everything else: a WASM filter can call the catalog,
read KV, mint a WI token, and emit mesh spans — so it's a *first-class mesh citizen*,
not a bolt-on.

> ★ Insight ─────────────────────────────────────
> The reason "arbitrary code between services" feels dangerous is that people picture
> **native code on a shared hot path with no limits** — which is genuinely a foot-cannon.
> The trick the whole industry converged on (Envoy proxy-wasm, Cloudflare Workers, Fastly
> Compute@Edge) is to change *two* variables at once: **sandbox the code** (WASM) **and
> bound it in time/space** (fuel + memory + a fail policy). Once both hold, "run my code
> between services" stops being reckless and becomes a feature — because the worst case
> is a single failed or passed-through request under an explicit `on_failure` policy,
> not a downed node. The job spec should make that safety envelope *non-optional*:
> there is no syntax to attach unbounded middleware.
> ─────────────────────────────────────────────────

### D6. Phasing

Slot this after the L7 waypoint (Phase 6 of the core plan): **Phase 7 — WASM middleware**
(proxy-wasm-style host ABI, fuel/memory limits, OCI artifact pull, per-route attach),
then **Phase 7b — native cdylib middleware** (operator-only, behind a cluster flag).

---

## Part E — Where this leaves the design

Service discovery (find + reach + authorize) is the **floor**; observability, traffic
management, and middleware are **opt-in ceilings** that reuse the same data path and the
same job-spec grammar. Nothing here forces a second system, a sidecar per task, or a
rewrite to "go mesh" — which is the whole point.
