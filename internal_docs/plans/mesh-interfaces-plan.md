# Plan: Mesh Interfaces — a Smaller Surface, One Handler Pipeline, and a Kernel Fast Path

> **Judgement calls:** [`calls/mesh-interfaces-plan_calls.md`](../calls/mesh-interfaces-plan_calls.md)

> **Status: PLANNED.** Nothing here is built. This is one plan for two
> questions about the mesh:
>
> 1. *Is the surface people type as small as it can be, and is the code
>    behind it pluggable?* Six surface changes (S1–S6): handlers as one typed
>    pipeline, upstreams as one `to`, the mesh as a switch on the service, one
>    caller list, one grammar for where material comes from, and a list of
>    knobs and kinds that go away.
> 2. *Can egress proxy code be both as cheap as possible and pluggable?* Two
>    datapath tiers: a kernel classifier that lets allowed flows skip the
>    proxy (A), and in-process WASM plugins (B).
>
> It merges two documents. One is the egress-plugins proposal, merged in
> [#49](https://github.com/mikenomitch/legion/pull/49) as
> `egress-plugins-proposal.md` and renamed to this file. The other is a review
> of the mesh's jobspec and code interfaces. The proposal's §6.E (interface
> reshape) is now split: keyed handlers, stage checks, and placement
> printing are in S1, and plugin objects are in B. The proposal's operator eBPF programs (its C), its
> file-descriptor hand-off (its D), kernel balancing (its A5), and `tc` for TAP
> sandboxes (its A6) are **cut from the phases** and listed in §6.F as
> on-demand work, so the plan ships less (calls entry 20). Calls entry 21
> records why this is one plan.
>
> It builds on, and does not replace, the shipped
> [service-mesh plan](service-mesh-plan.md),
> [network-handlers plan](network-handlers-plan.md), and
> [datapath-performance plan](datapath-performance-plan.md). B is the
> handlers plan's deferred phase 8 (WASM handlers), reshaped. A reopens the
> datapath plan's deferred "connect-time rewriting" (§6.7) with an answer to
> its objection, which was losing per-connection accounting and policy.
>
> Two findings are bugs, not design questions, and ship first on their own
> (phase 0, §8): `dlp` and `log` handlers declared on a chain pass validation
> and never run, and a chain's `onError` is shown by `net policy show` but
> never read by the datapath (§1, problems 6 and 7).

---

## 0. The short version

**The mesh has too many ways to say the same thing.** Three ways to restrict
callers, two shapes of upstream with four env vars each, three fields that
must agree to turn on the mesh, five spellings of "where a certificate or
secret comes from", two ways to write an access log, and four places that
decide whether a stream is HTTP. Each was added for a good reason. Together
they make the mesh harder to learn, and two of them have already drifted
apart from what the datapath does (§1).

**The spine is one handler pipeline (S1).** Today a handler's kind is a
string, matched separately by the validator, the L4 hop runner, HTTP mode,
and the manager. S1 makes every hop a typed variant behind one `Handler`
trait, with declared stages (`connect`, `request`, `body`, `response`,
`done`) and a declared placement (`kernel`, `netd`, `external`). A WASM
plugin (B) is one more implementation of that trait, not one more
dispatcher.

**The surface (S1–S6).** What job authors and operators type:

| | Change | Before | After |
| --- | --- | --- | --- |
| **S1** | Handlers are keyed variants on one pipeline | `{ handler = "authz", url = …, cache = … }`; `dlp` settings in untyped `config` | `{ authz = { url = …, cache = … } }`, `{ dlp = { patterns = […] } }` |
| **S2** | One `to` per upstream. Its scheme alone decides the protocol. One address to dial | `{ service = "db" }` or `{ name = "x", to = "https://…" }`; four env vars; two addresses | `{ to = "db" }`, `{ to = "http://search" }`, `{ name = "openai", to = "https://api.openai.com" }`; `LEGION_UPSTREAM_DB` is what you dial |
| **S3** | The mesh is a switch on the service. TLS lives on ports, not in policy chains | `ingress.tls = { mode = "mutual", certFrom = "legion", clientCa = "legion" }` | `service = { port = "http", mesh = true }`; `ports = [{ …, tls = { cert = { kv = … } } }]` |
| **S4** | One list of who may call a service | `service.allow` (job ids, endpoint feed only), `tls.requireIdentity` (identity globs), `ingress.rules[].when.identity` | `service.allow`, one identity grammar, enforced in the feed and, with the mesh, at the receiving node |
| **S5** | One grammar for where material comes from, and one fetch | `certFrom`, `credentials[].from`, `certPath`/`keyPath`, `clientCa`, `caPath`; three worker-plane fetch RPCs | `{ kv = … }`, `{ identity = … }`, `{ file = … }` everywhere; `FetchDatapathMaterial`; a `MaterialProvider` trait |
| **S6** | What goes away | — | the `log` handler, chain-level `onError`, `proxy.node`, `http.outlier`, the `random` balancer, the reserved `bridge`/`cni/*` modes (§6.0) |

**The datapath tiers (A, B).** Where the work runs:

- **eBPF decides whether a flow should exist, and where it goes.** It runs
  at `connect()` in the task's own cgroup, before a packet is sent. It can
  deny, **pass** (the flow never touches a proxy), or **redirect** into netd
  when something has to read the request.
- **eBPF cannot be the egress *proxy-code* plugin system.** It cannot read
  inside the task's TLS, hold a credential, make a call out, or buffer a
  body. Gated sandboxes have no host sockets for it to hook at all, because
  their traffic arrives over vsock. Request-level plugins have to live in
  netd, and the efficient form there is **in-process WASM**, not a webhook
  or a second proxy.

| | Tier | What it buys | Who writes the plugin code | Runs on |
| --- | --- | --- | --- | --- |
| **A** | Kernel classifier: policy compiled to eBPF maps | Allowed L4 flows skip the proxy entirely. Host-mode policy becomes enforced, not cooperative. Capture becomes transparent. Pass flows survive a worker restart | Nobody: the rules you already write compile to maps | Linux, `exec` and `container` |
| **B** | WASM plugins on the proxy-wasm ABI | Custom per-request logic in-process, at a cost of microseconds rather than an RPC or a second hop | Job authors and operators, in Rust, C++, or any language with a proxy-wasm SDK | Everywhere netd runs, sandboxes and macOS included |

**Order.** Phase 0 first, because it is two bugs. Then S1, because B plugs
into it and A reads its stages. After S1, every other track is independent:
S2, S3 + S4, S5, A, and B can ship in any order or in parallel. S6's
removals ride the track that rewrites the code they live in.

**Size of the surface, before and after:**

| Concept | Before | After |
| --- | --- | --- |
| Ways to restrict callers | 3 | 1 (`service.allow`; `rules[].when.identity` stays for identity + path) |
| Upstream shapes | 2 | 1 |
| Env vars per upstream | 4 | 3, all naming one address |
| Fields to turn on the mesh | 3, which must agree | 1 |
| Spellings of a material source | 5 | 1 |
| Worker-plane fetch RPCs for material | 3 | 1 |
| Places that decide "is this HTTP" | 4 | 1 (the upstream's scheme) |
| Chain fields | 8 | 6 (`tls` and `onError` move out) |
| Built-in handler kinds | 4 | 3, plus `plugin` |
| Balancers | 5 | 4 |
| Datapath plugin tiers in the proposal | 4 (A–D) | 2 (A, B) |

---

## 1. Problem

What a user hits today, concretely. Problems 1 to 5 are about cost and
pluggability on the egress path. Problems 6 to 11 are about the surface.

1. **Custom logic is expensive or impossible.** The built-in handler set is
   a closed table (`HANDLERS`,
   [src/structs/netpolicy.rs:67](../../src/structs/netpolicy.rs)): `authz`,
   `proxy`, `log`, `dlp`. Anything else has two routes:
   - **A webhook** (`authz`). One HTTP POST per new (identity, destination,
     method, path), cached, and verdict-only: it cannot rewrite a header,
     read a body, or count response tokens.
   - **A proxy** (`proxy`). A second userspace hop with a PROXY v2 header,
     plus another process to run, health-check, and roll out. On loopback
     one hop adds 4–7 µs per round trip on a kept-open connection and
     45–80 µs per new connection
     ([network-handlers.md](../../docs/network-handlers.md#what-the-datapath-costs)).
     A `proxy` handler adds a second hop on top of that.

   "Count each user's LLM tokens and cut them off at a budget" therefore
   means running Envoy or Squid as a service and pointing every chain at it.
2. **Every governed flow pays the hop, even when nothing reads it.** An
   allowed `CONNECT` to `pypi.org` through the gated proxy is accepted,
   judged, dialed, and spliced by the worker. Splice keeps bulk transfers
   cheap (0.09–0.11 CPU-seconds per GiB), but connection setup and per-round-
   trip latency are paid by every flow, for a decision made once at connect.
3. **Host-mode egress policy is cooperative.** A `host`-mode `exec` task that
   dials a raw address bypasses its own chain. The kernel guard
   ([kernel_guard.rs](../../src/worker/netd/kernel_guard.rs)) is a fixed
   floor (metadata, control ports, other allocations' listeners), not the
   task's policy.
4. **Gated capture depends on the client's cooperation.** A gated task
   reaches the network only through `HTTP_PROXY`/`ALL_PROXY` or its upstream
   ports. A tool that ignores the proxy variables fails, which enforces the
   policy but is a constant source of friction. Transparent capture (a TUN
   and a userspace stack) is deferred, and UDP is not implemented.
5. **A worker restart drops every in-flight flow**, because every flow is
   in the worker (network-handlers.md, "The datapath across worker
   restarts").
6. **A handler's kind is a string matched in four places, and two of them
   already disagree.** `HandlerConfig`
   ([netpolicy.rs:404](../../src/structs/netpolicy.rs)) is one flat
   struct. `url` and `cache` mean something only for `authz`. `service`,
   `port`, `node`, and `proxyProtocol` mean something only for `proxy`,
   where a count at validation enforces that exactly one target is set.
   `dlp`'s settings are untyped JSON in `config`, read once by the validator
   (`normalize_handler`, [netpolicy.rs:702](../../src/structs/netpolicy.rs))
   and again by HTTP mode, which drops a pattern that fails to compile with
   `filter_map` ([netd/http.rs:270](../../src/worker/netd/http.rs)). The
   kind string is matched by:
   - the validator;
   - `handlers::run` ([netd/handlers.rs:150](../../src/worker/netd/handlers.rs)),
     which serves `authz` and `proxy` and skips the rest;
   - `HttpSettings::new` ([netd/http.rs:235](../../src/worker/netd/http.rs)),
     which serves `dlp` and `log` and skips the rest;
   - the manager's `build_handler_endpoints`
     ([manager/server.rs:12822](../../src/manager/server.rs)), which reaches
     into `proxy`'s fields to build the endpoint feed, once for chains and
     once for upstreams.

   **The gap this leaves.** `HttpSettings` is only ever built from the
   upstream's own handlers (`&self.handlers`,
   [forwarder.rs:1119](../../src/worker/forwarder.rs)) or from none
   (`for_ingress`, [netd/http.rs:359](../../src/worker/netd/http.rs)).
   Nothing else compiles a chain's `dlp` or `log`. So a `dlp` or `log` on a
   chain, inline or on a policy object, passes validation and never runs. A
   `require` policy that tells every sandbox to block request bodies holding
   API keys does nothing. This was found by reading the code. Phase 0
   confirms it with a test before fixing it.
7. **A chain's `onError` is shown, not enforced.** The chain-level `onError`
   is validated, merged
   ([netpolicy.rs:1134](../../src/structs/netpolicy.rs)), and printed by
   `net policy show` as the default for any hop without its own
   ([cmd/net.rs:1084](../../src/cmd/net.rs)). The datapath reads only the
   hop's own value, and treats an absent one as `deny`
   ([netd/handlers.rs:144](../../src/worker/netd/handlers.rs),
   [netd/http.rs:292](../../src/worker/netd/http.rs)). A chain that says
   `onError = "allow"` is shown failing open and fails closed.
8. **An upstream is two shapes in one struct, and its protocol is decided in
   four places.** `UpstreamConfig`
   ([network.rs:688](../../src/structs/network.rs)) uses `service` for a
   catalog service and `name` + `to` for an external destination, held
   apart by a validation matrix
   ([network.rs:1812](../../src/structs/network.rs)). The resolved
   `AllocUpstream.service`
   ([network.rs:1097](../../src/structs/network.rs)) holds the upstream's
   key, which for an external upstream is not a service. HTTP parsing is
   turned on by any of:
   - `UpstreamConfig::is_http` ([network.rs:944](../../src/structs/network.rs)):
     an `http` block or an `http(s)://` scheme;
   - `declares_http` ([forwarder.rs:1094](../../src/worker/forwarder.rs)):
     the same, plus credentials, plus any `dlp` handler;
   - the task chain's `protocol`;
   - the **effective egress policy** ([forwarder.rs:1107](../../src/worker/forwarder.rs)).

   The last one means a pool-wide `protocol = "http"` policy, written for
   path rules, also parses a `{ service = "db" }` Postgres stream as HTTP and
   breaks it. That is documented. It also contradicts the handlers plan's
   rule that `protocol` is never inferred.
9. **One listener, two addresses, four env vars.** For one upstream on
   Linux:

   | What the task reads | Value |
   | --- | --- |
   | `LEGION_UPSTREAM_DB` | `127.0.0.1:20003`, always the loopback port ([network.rs:1236](../../src/structs/network.rs)) |
   | `LEGION_UPSTREAM_PORT_DB` | `20003` |
   | `LEGION_UPSTREAM_HOST_DB` | `db.svc.legion` |
   | `LEGION_UPSTREAM_VIP_DB` | `127.42.7.2:5432` |
   | `{{ service "db" }}` in a connect template | `127.42.7.2:5432` |

   The template helper's comment
   ([templates.rs:186](../../src/worker/templates.rs)) says it returns what
   `LEGION_UPSTREAM_<KEY>` carries. On Linux it does not. Every upstream
   also takes a port from the per-node 20000–32000 range, even where the VIP
   serves it.
10. **Turning on the mesh takes three fields in a policy chain, and "who may
    call me" has three answers.** The mesh is
    `ingress.tls = { mode = "mutual", certFrom = "legion", clientCa = "legion" }`,
    three fields that must agree. The catalog marks an endpoint `mesh_tls`
    only when the effective ingress chain resolves to the mesh at placement
    ([store.rs:13237](../../src/storage/store.rs),
    [store.rs:15142](../../src/storage/store.rs)). The values `tls.mode`
    accepts depend on direction
    ([netpolicy.rs:107](../../src/structs/netpolicy.rs)), and egress
    `originate` restates what `https://` on an upstream already says.
    Callers are restricted three ways:
    - `service.allow`: job ids, filtered in the endpoint feed
      (`allow_permits`, [network.rs:620](../../src/structs/network.rs),
      [server.rs:12896](../../src/manager/server.rs)). Not cryptographic, and
      never checked at the receiving node.
    - `tls.requireIdentity`: workload globs over
      `<region>:<job>:<group>:<task>`
      ([forwarder.rs:765](../../src/worker/forwarder.rs)), checked after the
      handshake ([netd/ingress.rs:463](../../src/worker/netd/ingress.rs)).
    - `ingress.rules[].when.identity`, plus `authz`.

    The public doc needs a paragraph ("Intentions … Scope") to explain how
    they differ.
11. **Every new source of material is a new RPC.** The same idea is spelled
    five ways: `certFrom = { kv }` ([netpolicy.rs:464](../../src/structs/netpolicy.rs)),
    `credentials[].from = { kv | identity }`
    ([network.rs:737](../../src/structs/network.rs)), `certPath`/`keyPath`,
    `clientCa = "legion" | <path>`, and `caPath`. The worker fetches policy
    bodies, KV certificates, and credentials through three worker-plane RPCs
    (`FetchNetPolicy`, `GetIngressCertificates`, `GetUpstreamCredentials`;
    [legion.proto:1855, 1137, 1185](../../proto/legion.proto)). Each has its
    own `NetdWants` field and `NetdInput` variant
    ([netd/datapath.rs:64, 96](../../src/worker/netd/datapath.rs)). A Vault or
    cloud secret-manager source would need a new RPC, a new wants field, a new
    input variant, a new caller class, and a new `tests/acl_endpoints.rs` row.
    `clientCa = { kv }` is still an open TODO item for the same reason.

## 2. Prior art

- **Cilium.** L3/L4 policy is held in eBPF maps and evaluated in the
  kernel. Socket-level load balancing rewrites a service VIP to a backend at
  `connect()`, so there is no per-packet NAT and no proxy hop. **FQDN
  policy** (`toFQDNs`) runs DNS through a proxy that records name→IP
  mappings, then allows those IPs in the kernel. Traffic goes to a userspace
  proxy (Envoy) only when an L7 rule exists
  ([docs](https://docs.cilium.io/en/stable/security/dns/)). *Take:* policy
  as maps, IPs learned from DNS, and redirect only when needed. *Reject:*
  CNI, an overlay, and Envoy as a second datapath.
- **Merbridge (Istio).** This replaces iptables redirection with eBPF. A
  `cgroup/connect4` program rewrites the destination to the proxy and saves
  the original destination under the socket cookie. A `cgroup/getsockopt`
  program serves it back as `SO_ORIGINAL_DST`, so Envoy needs no change
  ([Istio blog](https://istio.io/latest/blog/2022/merbridge),
  [walkthrough](https://labs.iximiuz.com/tutorials/ebpf-envoy-egress-dc77ccd7)).
  Its documented caveat is that the programs apply node-wide and have to
  filter out traffic that is not in the mesh. *Take:* the redirect
  mechanism. *Avoid the caveat:* Legion attaches to each allocation's own
  cgroup leaf, which it already creates.
- **Envoy / proxy-wasm.** WASM filters against a host ABI, version 0.2.1
  being the widely implemented one, with HTTP-filter and network-filter
  entry points ([spec](https://github.com/proxy-wasm/spec),
  [Envoy](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/advanced/wasm)).
  *Take:* the ABI subset for HTTP filters, so existing SDKs and filters
  work.
- **Envoy's extension registry.** Every filter is a name plus a
  `typed_config` checked against that filter's own schema, built through a
  factory registry. *Take:* typed settings per kind, resolved through one
  registry. *Reject:* the `@type` URL ceremony. A keyed Nickel record reads
  better.
- **Istio ambient.** A per-node L4 proxy (ztunnel) plus an optional per-
  service L7 proxy (waypoint). *Take:* L7 cost only where L7 is asked for.
  *Reject:* a second proxy component (service-mesh calls entry 3 already
  made this call).
- **Consul service-defaults and intentions.** A service's protocol (`tcp`,
  `http`, `grpc`) is declared once in `service-defaults`, not inferred from
  policy. Intentions are the one "who may call me" surface: L4 by service
  identity, L7 by path, method, and header once the protocol is HTTP,
  enforced by the destination's proxy. *Take:* the protocol declared on the
  thing being dialed, and one caller list checked at the receiving side.
  *Reject:* config entries as another object kind to manage.
- **Kubernetes.** `NetworkPolicy` is L3/L4 only, enforced by whatever CNI
  is installed, and has no FQDN rules in core. The extension model is
  "replace the CNI." A `Service` port carries its protocol in `appProtocol`,
  and Gateway API's `BackendTLSPolicy` attaches TLS to the backend, not the
  route. **bpfman** treats eBPF programs as cluster objects that a
  privileged daemon loads, gated by RBAC so applications need no privilege
  ([bpfman](https://next.redhat.com/project/bpfman/)). *Take:* protocol and
  TLS live on the port, and a kernel program is an operator-only object if
  it is ever allowed (§6.F).
- **Linkerd.** It detects the protocol of each connection, and documents
  "opaque ports" as the escape hatch for protocols where the server speaks
  first, which detection gets wrong. Its `Server` and `AuthorizationPolicy`
  authorize callers per port by mTLS identity, at the destination. *Take:*
  authorization at the destination. *Reject:* detection. S2's whole point is
  that the declared scheme is the protocol.
- **Nomad + Consul.** Envoy sidecars, with transparent proxy by a CNI
  plugin that installs iptables rules. An upstream is one shape
  (`destination_name`, `local_bind_port`). There is no plugin point in the
  datapath besides Envoy's own configuration. *Take:* one upstream shape.
  *Reject:* a sidecar per task.
- **Kubernetes Secrets Store CSI driver.** One mount API for consumers.
  Vault, AWS, Azure, and GCP are providers behind a gRPC provider
  interface. *Take:* one source grammar for the consumer, and providers
  behind a trait on the server.
- **Cloudflare `sk_lookup`.** Steers connections for any address and port
  to one listening socket without iptables, and the accepted socket keeps
  the original destination as its local address. This is the alternative
  capture mechanism, considered in calls entry 18.

## 3. Jobs to be done

**Job author / app developer**
- When my task calls an external API, I want to add a small piece of logic
  to every request (a header, a check, a count) without running a second
  proxy, so my calls stay fast and I have nothing extra to operate.
- When my task downloads gigabytes from a registry it is allowed to reach,
  I want the platform to stay out of the way, so the download runs at full
  speed.
- When a tool inside my task ignores proxy settings, I want it to just work
  under the same policy, so I don't have to patch every tool.
- When I connect my service to a database, I want to write the address once
  and have every tool in my task use the same one, so a config file and an
  environment variable can't point at different places.
- When I put my service on the encrypted network, I want to flip one
  switch, so I can't turn it half on.
- When I put a setting in the wrong place, I want the error to name the
  thing I was configuring and the field, so I fix it before I deploy.

**Security / platform admin**
- When I say "no SMTP from the free tier", I want that enforced on every
  matching task, including tasks on host networking, so a raw socket can't
  route around it.
- When a job author ships a plugin, I want it unable to see other tenants'
  traffic or our API keys, so letting authors extend the datapath doesn't
  widen what a tenant can reach.
- When I write down which services may call ours, I want one list that holds
  both in the control plane and on the wire, so I don't write it three ways
  and hope they agree.
- When I apply a policy that inspects request bodies, I want it to run
  everywhere it matches or be refused, so a policy that looks enforced is
  enforced.
- When I tighten web egress for a whole pool, I want our database
  connections to keep working, so a policy aimed at HTTP doesn't break
  Postgres.

**Cluster operator**
- When I upgrade or restart a worker, I want running tasks' connections to
  survive, so a deploy isn't an outage for long-lived streams.
- When a plugin is slow or broken, I want it contained and named, so one
  bad plugin can't take down the node's networking.
- When our certificates and keys live in another secret store, I want
  supporting it to be one well-defined piece of work, so it is a small
  contribution and not a redesign.

**Autonomous agent**
- When I write a plugin, I want to test it against a sample request without
  a cluster, and dry-run applying it, so I can fix it before it touches
  traffic.
- When my request is refused, I want to read which rule or plugin refused it
  and why, so I can change course instead of retrying.
- When I read a task's environment, I want one variable per upstream that is
  the address to dial, so I don't have to know which of four to use.

*Not in scope:* the on-call responder gets nothing new beyond `net doctor`
naming where each rule runs. The incident tooling that exists already
covers them.

### User scenarios

1. **A per-user token budget on an agent platform.** Free-tier sandboxes
   call `api.openai.com` through an external `https://` upstream, which is
   already plaintext to the node. An operator applies a `llm-token-meter`
   WASM plugin to `tier = "free"`. It reads each streamed response, adds up
   the usage per `labels.user`, and returns a structured `429` once the
   budget is spent. There is no second proxy and no webhook (B).
2. **Closing SMTP on a batch pool.** A policy object for pool `batch` denies
   ports 25, 465, and 587 and allows `*.pypi.org`. A host-mode `exec` task
   that runs `nc mail.example.com 25` gets `EPERM` at once, and the
   `net.denied` event names the rule. Its `pip install` connects straight to
   PyPI's CDN with no proxy in the path (A).
3. **A tool that ignores `HTTPS_PROXY` in a gated container** reaches
   `github.com` because policy allows it, and is refused for `evil.example`.
   Nothing in the image was changed (A, transparent capture).
4. **An agent writes a header-rewriting plugin.** It runs
   `legion net plugin new`, edits one function, runs
   `legion net plugin test` against a synthetic request, then
   `legion net plugin apply --dry-run`, then applies it (S1, B).
5. **Putting `api` on the mesh and letting only `web` call it.** Today:

   ```nickel
   service = { name = "api", port = "http", allow = ["web"] },
   network = {
     ports = [{ name = "http" }],
     ingress = { tls = { mode = "mutual", certFrom = "legion", clientCa = "legion",
                         requireIdentity = ["*:web:*:*"] } },
   },
   ```

   After S3 and S4:

   ```nickel
   service = { name = "api", port = "http", mesh = true, allow = ["web"] },
   network = { ports = [{ name = "http" }] },
   ```

   The consuming `web` task's datapath originates mesh TLS. A `batch` task
   gets an empty endpoint list from the feed. A non-Legion client that dials
   the port directly is refused at the handshake. `legion net policy show api`
   prints the compiled rule with the source `service/allow`.
6. **An HTTP policy on a pool that runs a database.** An operator applies:

   ```nickel
   { name = "prod-web", selector = { nodePools = ["prod"] }, enforcement = "require",
     egress = { protocol = "http", allow = ["db", "api.github.com/repos/**"] } }
   ```

   Today, every upstream on `prod` is parsed as HTTP, so `{ service = "db" }`
   breaks on the Postgres greeting. After S2, `db` (written `{ to = "db" }`)
   stays L4: the `db` rule allows it at connect time and the bytes are
   spliced. A `tcp://` upstream whose verdict needs a path is denied with a
   reason that names the upstream, and `legion job validate` warns before
   submit (§6.S2).

## 4. Goals and non-goals

**Goals**
- **One spelling per concept:** one handler grammar, one upstream shape,
  one mesh switch, one caller list, one source grammar.
- **A declared protocol is the protocol.** No field silently turns on HTTP
  parsing, and no policy changes how a stream is parsed.
- **Every hop a policy declares either runs where it matches or is refused
  at submit.** Validation, `show`, and the datapath read one registry.
- **Adding a handler kind, a plugin runtime, or a secret provider is one
  implementation plus one registry row.**
- Allowed L4 flows that nothing needs to read cross **no proxy** on Linux
  host-mode tasks.
- The egress chain is **enforced** for host-mode tasks on capable Linux
  nodes, and gated capture is **transparent**.
- Custom logic runs **at the stage it needs** (connect, request, body,
  response) and **in-process** where it reads requests, at microseconds per
  call. Every plugin is sandboxed, has a deadline, and fails closed.
- Plugins are **easy to write**: a mainstream language and an existing SDK,
  a scaffold, a local test with no cluster, apply as an object, and see
  where it runs.
- The same policy is enforced at the **cheapest point each runtime
  allows**, and `show`/`doctor` say which point that is.

**Non-goals**
- **Compatibility aliases for the old shapes.** AGENTS.md: break the surface
  when it gives a cleaner one, and say what broke (§10).
- **Protocol detection.** The scheme decides (calls entry 6).
- **Per-workload certificates.** The mesh stays node certificates with
  attributed workload identity.
- **New secret providers themselves** (Vault, cloud secret managers). S5
  makes each one implementation. Each is its own TODO item.
- **The mesh on by default.** It stays off (calls entry 8).
- **Kernel code from anyone.** No job author or operator eBPF programs in
  this plan (§6.F, calls entries 2 and 20).
- **TLS interception.** It stays rejected. External upstreams remain the
  body-visible route.
- **Replacing netd** with Envoy, Pingora, or a CNI. There is one datapath.
- **HTTP/2, kTLS, and the `legion netd` process split.** These are separate
  items. Nothing here makes them harder, and S5 makes the split's message set
  smaller.
- **Kernel enforcement on macOS, or inside sandbox guests.** The guest is
  untrusted, so enforcement stays on the host side of the vsock.
- **Rate limits and quotas as a rule action.** A plugin can do this
  (scenario 1). A first-class `rateLimit` action is a separate decision.
- **Unifying connect templates with KV templates.** Two template engines
  exist: the manager renders `{{ kv }}` into env and files, and the worker
  renders `connect.templates` from the endpoint feed. Merging them is a real
  simplification, but it reshapes the KV injection surface too, so it is its
  own TODO item, not part of this plan.

## 5. What exists today

| What | Where | Why it matters here |
| --- | --- | --- |
| The L4 forwarder and `ForwarderContext` (policy lookup, denials, credentials, handler state) | [forwarder.rs:226](../../src/worker/forwarder.rs) | The redirect target for A, and the host for B |
| `splice(2)` for bulk L4, with a copy fallback | [netd/splice.rs:93](../../src/worker/netd/splice.rs) | Redirected flows keep it |
| Connect-time and request-time evaluators | `evaluate_connect` / `evaluate_request`, [netpolicy.rs:1309](../../src/structs/netpolicy.rs). `Verdict::Defer` when a connect-time decision needs the request head ([netpolicy.rs:1221](../../src/structs/netpolicy.rs)) | The single source of truth that the kernel maps must agree with (§9), and what S2's fail-closed rule reads |
| `HandlerConfig`, a flat bag: `url` only means something for `authz`, `service` only for `proxy` | [netpolicy.rs:404](../../src/structs/netpolicy.rs), validated by `normalize_handler` at [:702](../../src/structs/netpolicy.rs) | What S1 replaces |
| Hop dispatch, in four places | `handlers::run` ([netd/handlers.rs:150](../../src/worker/netd/handlers.rs)), `HttpSettings::new` ([netd/http.rs:235](../../src/worker/netd/http.rs)), `build_handler_endpoints` ([server.rs:12822](../../src/manager/server.rs)), the validator | The four places S1 replaces with one registry and one compiled pipeline |
| The `Balancer` trait and the `BALANCERS` table, read by validation too | [balance.rs](../../src/worker/balance.rs) | The pattern S1 copies for handlers |
| `UpstreamConfig`, its validation matrix, `AllocUpstream` | [network.rs:688](../../src/structs/network.rs), [:1812](../../src/structs/network.rs), [:1097](../../src/structs/network.rs) | What S2 replaces |
| HTTP-mode triggers | [network.rs:944](../../src/structs/network.rs), [forwarder.rs:1094](../../src/worker/forwarder.rs), [:1107](../../src/worker/forwarder.rs) | The four triggers S2 reduces to one |
| Upstream env vars and the template address | `env_pairs`, [network.rs:1218](../../src/structs/network.rs); `service_address`, [templates.rs:186](../../src/worker/templates.rs) | S2's one-address rule |
| Mesh opt-in | `TlsConfig` and `cert_from_mesh`, [netpolicy.rs:437](../../src/structs/netpolicy.rs), [:481](../../src/structs/netpolicy.rs); `ingress_tls_at_placement`, [store.rs:13237](../../src/storage/store.rs); the catalog's `mesh_tls`, [store.rs:15142](../../src/storage/store.rs) | What S3 collapses to `service.mesh` |
| Callers | `allow_permits` in the endpoint feed, [network.rs:620](../../src/structs/network.rs), [server.rs:12896](../../src/manager/server.rs); `requireIdentity`, [netd/ingress.rs:463](../../src/worker/netd/ingress.rs); the identity string, [forwarder.rs:765](../../src/worker/forwarder.rs) | What S4 unifies |
| Material sources and fetches | `CertFrom`, [netpolicy.rs:464](../../src/structs/netpolicy.rs); `CredentialFrom`, [network.rs:737](../../src/structs/network.rs); `NetdInput`/`NetdWants`, [netd/datapath.rs:64](../../src/worker/netd/datapath.rs), [:96](../../src/worker/netd/datapath.rs); the three RPCs, [legion.proto:1137, 1185, 1855](../../proto/legion.proto) | What S5 replaces |
| HTTP mode: rules, hops, credentials, built-in `dlp`/`log` | [netd/http.rs:731](../../src/worker/netd/http.rs) (`handle`), `:270` | The request, body, and response stages B hooks |
| The gated egress proxy (`CONNECT`, absolute URI, SOCKS5) | [netd/proxy.rs](../../src/worker/netd/proxy.rs) | Keeps working; transparent capture sits beside it |
| The per-allocation resolver, which already judges names against policy | [netd/dns.rs:431](../../src/worker/netd/dns.rs) | The source of kernel FQDN policy (§6.A.4) |
| The kernel guard: one `nftables` table matching sockets by cgroup leaf | [netd/kernel_guard.rs](../../src/worker/netd/kernel_guard.rs) | Proves the per-allocation cgroup attach point. Phase A1 re-implements it in eBPF |
| Every task gets a cgroup leaf `legion/{share,reserve}/legion-<alloc>`, even with resource enforcement off | [oci_bundle.rs:1679](../../src/task_plugins/oci_bundle.rs), [resources.rs](../../src/worker/resources.rs) | The attach point for every hook in A |
| Gated namespaces: loopback plus listeners | [netd/netns.rs:58](../../src/worker/netd/netns.rs) | Stay the boundary. A adds transparency, not a replacement |
| Restart replay from `netd_state.json` | [netd/state.rs](../../src/worker/netd/state.rs) | Pinned eBPF links extend this to the kernel tier |

---

## 6. Proposed design

Part one is the surface (S1–S6). It changes what people type and how the
code behind it is organized. Part two is the datapath tiers (A, B). It
changes where the work runs. §6.0 lists everything the plan removes, in one
place.

### 6.0 What goes away

Every removal below either duplicates something that stays or is a knob
whose default is right for almost everyone. Each rides the track that
already rewrites the code it lives in (calls entry 17).

| Removed | Why | Instead | Track |
| --- | --- | --- | --- |
| `log` handler | Does exactly what `accessLog = true` does (`access_log` checks `chain.access_log \|\| settings.log_handler`, [netd/http.rs:1703](../../src/worker/netd/http.rs)) | `accessLog = true` on the chain | S1 |
| Chain-level `onError` | Shown by `show`, never read by the datapath (problem 7) | `onError` on the hop, default `deny` | S1 |
| `handler = "…"` string and `config = {…}` | Replaced by keyed variants | `{ authz = {…} }`, `{ dlp = {…} }` | S1 |
| `proxy` target `node = "x"` | It is "`service = "x"`, node-local endpoints only" under another name | `{ proxy = { service = "x", local = true } }` | S1 |
| `service`, `localPort` on upstreams | One `to`; `port` pins | `{ to = "db" }`, `{ to = "cache", port = 6379 }` | S2 |
| `LEGION_UPSTREAM_VIP_<K>` | It names the same listener as `LEGION_UPSTREAM_<K>` | `LEGION_UPSTREAM_<K>` | S2 |
| `http.outlier` | `least_latency` already counts a 5xx as a 1-second answer and sheds the endpoint. Ejection becomes fixed behavior in HTTP mode for every balancer: 5 consecutive 5xx, 30 s out, today's defaults | nothing to write | S2 |
| `random` balancer | It exists so that many clients don't send their first connections to the same endpoint. That happens with `round_robin` because every new listener's cursor starts at 0 ([balance.rs](../../src/worker/balance.rs)). Starting each listener's cursor at a random offset fixes `round_robin` itself, and then `random` adds nothing (calls entry 17) | `round_robin` | S2 |
| `tls.mode` on chains (`off`/`server`/`mutual`/`passthrough`/`originate`), `certFrom`, `certPath`/`keyPath`, `clientCa`, `caPath`, `requireIdentity` | The mesh is one switch, and TLS moves onto ports and upstreams (S3). Callers move to `service.allow` (S4) | `service.mesh`, `ports[].tls`, upstream `ca`, `service.allow` | S3, S4 |
| Reserved `network.mode` values `bridge` and `cni/<name>` | Reserving a name for a plugin that does not exist is surface without a feature ([netpolicy.rs:903](../../src/structs/netpolicy.rs)) | The unknown-mode error lists the valid modes | S3 |
| `FetchNetPolicy`, `GetIngressCertificates`, `GetUpstreamCredentials` | One fetch, keyed by kind (S5) | `FetchDatapathMaterial` | S5 |

The registries keep re-adding cheap. A balancer that comes back is one row
in `BALANCERS`, and a handler kind is one row in `HANDLERS` (§6.S1).

### 6.S1 One handler pipeline

**Surface.** A handler entry is a keyed variant, like `from = { kv = … }`
and `certFrom = { kv = … }` already are. `timeout` and `onError` stay on
the entry, and each variant gets its own Nickel contract, so a misplaced
`url` fails at evaluation with the variant named.

Before:

```nickel
egress = {
    protocol = "http",
    onError = "allow",                          # shown by `show`, ignored by the datapath
    handlers = [
        { handler = "authz", url = "http://policy.internal/check", cache = "30s", onError = "deny" },
        { handler = "proxy", node = "egress-filter", proxyProtocol = true },
        { handler = "dlp", config = { maxBytes = "1Mi", patterns = ["(AKIA|sk-)[A-Za-z0-9]{16,}"] } },
        { handler = "log" },
    ],
}
```

After:

```nickel
egress = {
    protocol = "http",
    accessLog = true,
    handlers = [
        { authz = { url = "http://policy.internal/check", cache = "30s" }, onError = "deny" },
        { proxy = { service = "egress-filter", local = true, proxyProtocol = true } },
        { dlp = { maxBytes = "1Mi", patterns = ["(AKIA|sk-)[A-Za-z0-9]{16,}"] } },
        { plugin = "llm-token-meter", config = { budget = 2000000, per = "labels.user" }, timeout = "2ms" },   # B
    ],
}
```

The `proxy` target is one of `service` (optionally `local = true`) or `port`
(a sidecar's port label), enforced by the type rather than by a count.

**The Rust shape.**

```rust
/// One hop as written: the kind, plus what every hop shares.
pub struct HopConfig {
    pub kind: HandlerKind,      // externally tagged: `{ authz = {...} }`
    pub timeout: Duration,      // default 2s
    pub on_error: OnError,      // default deny
}

pub enum HandlerKind {
    Authz(AuthzConfig),
    Proxy(ProxyConfig),         // target: Service { name, local } | Port(label)
    Dlp(DlpConfig),             // patterns: Vec<Regex>, compiled once, at validation
    Plugin(PluginRef),          // B1
}

pub enum Stage { Connect, Request, Body, Response, Done }
pub enum Placement { Kernel, Netd, NetdWasm, External }

#[async_trait]
pub trait Handler: Send + Sync {
    /// The stages it acts at. A request-or-later stage on a path that does
    /// not parse HTTP is a submit error; it never turns HTTP mode on.
    fn stages(&self) -> &'static [Stage];
    fn placement(&self) -> Placement;
    /// Catalog services this hop dials. The manager's endpoint feed asks
    /// this instead of matching on the kind.
    fn endpoint_needs(&self) -> Vec<String> { Vec::new() }
    async fn on_connect(&self, cx: &mut ConnectCx<'_>) -> HopOutcome { HopOutcome::Allow }
    async fn on_request(&self, cx: &mut RequestCx<'_>) -> HopOutcome { HopOutcome::Allow }
    async fn on_body(&self, cx: &mut BodyCx<'_>) -> HopOutcome { HopOutcome::Allow }
    async fn on_response(&self, cx: &mut ResponseCx<'_>) -> HopOutcome { HopOutcome::Allow }
    async fn on_done(&self, cx: &DoneCx<'_>) {}
}

/// The only list of handler kinds. Validation, the datapath, `show`, and the
/// manager read it, so a kind cannot be accepted and then not run.
pub const HANDLERS: &[HandlerEntry] = &[ /* authz, proxy, dlp, plugin */ ];
```

This is the pattern `BALANCERS` in [balance.rs](../../src/worker/balance.rs)
already proved: one table, read by validation too, so there is no second
place to forget.

**One compiled pipeline.** When a policy body arrives (once per hash) and
when an upstream's spec is built, the worker compiles the chain's hops and
then the upstream's hops into one `Arc<Pipeline>`: the hop list plus derived
facts (`needs_http`, `needs_body`, `needs_netd`, `endpoint_needs`). Every
path runs that same pipeline, calling the stages it can see: L4 upstream
splices, HTTP-mode upstreams, the gated proxy (`CONNECT`, absolute URI,
SOCKS5), and ingress, L4 and HTTP. There is no second list for a path to be
handed or not handed, which is what closes problem 6 for good.

**Kinds a path cannot serve are refused at submit**, the way a path rule on
a `tcp` chain is today:

```
egress.handlers[0] dlp inspects request bodies at stage "body", but
egress.protocol is "tcp". Set protocol = "http", or put it on an upstream
whose `to` is http:// or https://.
```

```
connect.upstreams entry 'db' handlers[0] plugin "llm-token-meter" runs at
stage "response", but upstream 'db' is tcp://. Write to = "http://db" if it
speaks HTTP.
```

**Every rule and hop says where it runs.** `show`, `doctor`, and the UI
label each one `kernel`, `netd`, `netd (wasm)`, or `external`, from
`Handler::placement`, and give the chain's flow path (`pass`, `redirect`, or
`netd`). The handlers plan holds that `protocol` is never inferred, so that a
reviewer can read the cost straight off the policy. Placement keeps that
property by being printed, not implied.

**Adding a kind** is one file under `src/worker/netd/handlers/`, one
`HANDLERS` row, one Nickel contract, one entry in `configs/jobs/maximal.ncl`,
and one row in the public handler table. A parity test fails the build if any
of the last four is missing (§9).

### 6.S2 Upstreams: one `to`, the scheme decides, one address

**Surface.**

```nickel
connect = {
    upstreams = [
        { to = "db" },                                         # catalog service, L4
        { to = "http://search", http = { retries = { attempts = 2 } } },   # catalog service, HTTP
        { to = "cache", port = 6379 },                         # pinned port; also binds 127.0.0.1:6379
        { name = "openai", to = "https://api.openai.com",      # external; the node opens TLS
          credentials = [{ header = "Authorization", prefix = "Bearer ", from = { kv = "secrets/openai" } }] },
        { name = "corp", to = "https://api.corp.internal", ca = { file = "/etc/corp/ca.pem" } },
    ],
},
```

| `to` | Destination | Protocol | Key (`LEGION_UPSTREAM_<KEY>`) |
| --- | --- | --- | --- |
| `db`, `tcp://db`, `db.svc.legion` | catalog service `db` | L4 | `name`, default `DB` |
| `http://search` | catalog service `search` | HTTP | `name`, default `SEARCH` |
| `tcp://10.0.0.5:5432`, `tls://ledger.example:7000`, `http://intranet.example`, `https://api.openai.com` | external | by scheme | `name`, required |

- **Catalog or external.** A host that is a bare service name (no dot, no
  port) or ends in `.svc.legion` is a catalog service. Anything else is
  external. A dotless external host is written with its port
  (`http://wiki:80`). `db.svc.legion` is accepted but never required
  (calls entry 14).
- **Catalog services take `tcp` or `http`.** They are reached over the mesh
  when the service is on it (S3), so `tls://` and `https://` name external
  destinations only:

  ```
  task api connect.upstreams[1] to = "https://search": catalog services are
  reached over the mesh, so tls:// and https:// name external destinations.
  Hint: write to = "http://search", and put the search service on the mesh
  (service.mesh = true).
  ```
- **The fields that remain:** `to`, `name`, `port`, `balance`, `http`
  (`timeout`, `retries`, `split`), `handlers`,
  `credentials`, and `ca` (an external TLS upstream's trust bundle, as an S5
  source). `AllocUpstream.service` becomes `key`.

**The protocol rule.** An upstream's scheme is its protocol, and nothing
else changes it:
- Credentials already require `https://`.
- A handler that needs a later stage than `connect` on a `tcp` upstream is a
  submit error (§6.S1).
- An `http` block on a `tcp` upstream is a submit error:

  ```
  task api connect.upstreams[0] to = "db" is reached over L4, but it has an
  http block. Hint: write to = "http://db" if it speaks HTTP.
  ```
- **A chain's `protocol` no longer changes how a stream is parsed.** It says
  which rules may be written (path, method, header), which is what it says
  today. Those rules apply to HTTP upstreams and to the gated proxy's
  absolute-URI requests. On a `tcp` upstream the chain is evaluated with
  connect-time facts. A verdict that needs the request head
  (`Verdict::Defer`) is a denial that names both sides:

  ```
  net.denied  policy/prod-web#1  upstream 'db' is tcp://, and the first rule
  that could match it needs a path. Hint: allow 'db' by host ahead of path
  rules, or write to = "http://db" if it speaks HTTP.
  ```

  `legion job validate` and `legion net policy apply --dry-run` warn when a
  `tcp` upstream sits under an HTTP chain whose verdict for it cannot be
  decided at connect time. The outcome matches today's in practice (an HTTP
  parser fails on a Postgres or TLS stream and the connection dies), but now
  it is stated and explained instead of accidental. Calls entry 6 records the
  trade-off.

For A, this makes the redirect rule exact. A flow needs netd when its
declared protocol is HTTP or its pipeline has a stage later than `connect`
(§6.A.3).

**One address per upstream** (calls entry 15).

| Variable | Linux | macOS (and any node without a VIP block) |
| --- | --- | --- |
| `LEGION_UPSTREAM_<K>` | `127.42.7.2:5432`: the VIP and the canonical port | `127.0.0.1:20003` |
| `LEGION_UPSTREAM_HOST_<K>` | `127.42.7.2` | `127.0.0.1` |
| `LEGION_UPSTREAM_PORT_<K>` | `5432` | `20003` |

- `{{ service "db" }}` in a connect template and the variable come from one
  function, so they cannot disagree.
- `<key>.svc.legion` still resolves through the allocation's resolver. A
  `container` task gets that resolver in its generated `resolv.conf`. An
  `exec` task gets it once A2 captures DNS, which is why the variables carry
  addresses, not names.
- **On Linux only the VIP is bound by default,** so no port is taken from
  the dynamic range. `port = N` pins the canonical port and also binds
  `127.0.0.1:N`, for a config file that needs a literal loopback address.
  Where the node has no VIP block to give (macOS, or the blocks are used up),
  the loopback port is the address and the variables say so.

**Balancing and HTTP traffic management** keep `balance` (now
`least_latency`, `round_robin`, `least_conn`, or `local_first`), `timeout`,
`retries` (all of today's fields), and `split`. `round_robin` and
`least_conn`'s tie-break start each listener's cursor at a random offset.
Outlier ejection is fixed behavior in HTTP mode for every balancer (§6.0).

### 6.S3 The mesh is a switch on the service; TLS lives on ports

**Surface.**

```nickel
service = { port = "http", mesh = true },
```

`mesh = true` means what the three-field form means today:
- the advertised port terminates node-to-node mTLS with the node
  certificate and verifies peers against the cluster CA;
- the catalog marks the endpoints mesh, and consumers originate mesh TLS;
- a plain client that dials the port directly is refused at the handshake.

Like `mode`, it is resolved at placement, because the catalog's flag and the
listener must agree. A policy object can impose it with `mesh = true` under
`enforcement = "require"`, or default it under `"default"`, with the same
merge rules as `mode`.

**TLS that is not the mesh** goes on the port that serves it:

```nickel
network = {
    ports = [
        { name = "https", static = 443, tls = { cert = { kv = "certs/app.example.com" } } },
        { name = "admin", to = 9443,
          tls = { cert = { file = { cert = "/etc/app/tls.crt", key = "/etc/app/tls.key" } },
                  clients = { ca = { kv = "certs/admin-clients" } } } },   # present = mutual
    ],
},
```

- A port is either the service's mesh port or carries its own `tls`, never
  both. Validation says so.
- `clients = { ca = … }` makes the port mutual. The verified peer's identity
  is its first URI SAN, else its first DNS SAN, else its CN, as today, and
  `ingress.rules[].when.identity` and `authz` see it.
- `clients.ca = { kv }` is what S5 gives every source, and it closes the open
  TODO item for a KV-held client CA.
- On egress, `tls.mode = "originate"` and `caPath` go away. An external
  `https://` or `tls://` upstream already originates, and its trust bundle is
  the upstream's `ca` (§6.S2).

**Chains carry only policy:** `protocol`, `policy`, `accessLog`, `allow`,
`rules`, and `handlers`. Calls entry 8 records what this costs. A policy
object can no longer push certificate material onto the tasks it matches.
It can impose `mesh = true`.

### 6.S4 One list of who may call a service

**Surface.**

```nickel
service = {
    port = "http",
    mesh = true,
    allow = ["web", "agent-*", "*:billing:*:worker"],
},
```

- **One grammar.** A bare entry is a job id glob (`web`, `agent-*`), which is
  today's meaning. An entry with colons is a workload identity glob over
  `<region>:<job>:<group>:<task>` (`*:billing:*:worker`). Both compile to one
  matcher on the identity string, so `web` and `*:web:*:*` are the same rule.
  Absent, or `["*"]`, means everyone, as today.
- **Enforced in the control plane, always.** The endpoint feed filters per
  consuming allocation, as `allow_permits` does today, now with the same
  matcher. A consumer that is not on the list gets an empty endpoint list and
  a fast close.
- **Enforced on the wire, with the mesh.** On a `mesh = true` service,
  `allow` compiles into the service's ingress chain as the first rule, with
  the source `service/allow`. A verified peer outside it is refused with
  `Legion-Denied-By: service/allow` and a `net.denied` event. This is what
  `requireIdentity` did, now without a second list.
- **Without the mesh** there is no verified identity at the receiving node,
  so `allow` is enforced by the control plane only. `legion service doctor`
  says so in one line:

  ```
  allow: web, agent-*   enforced by the endpoint feed only (api is not on the mesh)
    Hint: set service.mesh = true to enforce it on the wire
  ```
- `ingress.rules[].when.identity` stays, for identity combined with a path
  or method. `legion net policy test api --from web --identity …` answers for
  the whole set.

### 6.S5 One source grammar, one material fetch

**Surface.** Everywhere a value comes from somewhere, it is the same record:

| Where | Before | After |
| --- | --- | --- |
| A port's certificate | `certFrom = { kv }`, or `certPath` + `keyPath` | `tls.cert = { kv = … }` or `{ file = { cert, key } }`. The mesh is S3's switch |
| A mutual port's client CA | `clientCa = "legion"` or a path | `tls.clients.ca = { kv = … }` or `{ file = … }` |
| An external upstream's CA | `tls.caPath` on the egress chain | the upstream's `ca = { file = … }` or `{ kv = … }` |
| An injected credential | `from = { kv }` or `{ identity }` | unchanged. This is the model the rest adopt |

```rust
pub enum Source {
    Kv(String),           // the manager reads it; needs kv:read at submit, as today
    Identity(String),     // a workload-identity token minted for the allocation
    File(FileSource),     // read by the node itself; never sent over the wire
}
```

Each field allows the kinds that make sense for it. An identity token is not
a certificate, so `tls.cert = { identity = … }` is a submit error, and
credentials take no `file`, so a node's disk is never a secret store (open
question 8).

**One fetch.** `FetchDatapathMaterial` replaces `FetchNetPolicy`,
`GetIngressCertificates`, and `GetUpstreamCredentials`:

```proto
// Fetch the material a node's datapath needs for the allocations placed on
// it: policy bodies by hash, KV-held certificates and CA bundles, injected
// credentials, and (B1) plugin module digests.
//
// ACL: worker plane (node certificate). Every ref is checked as the three
// RPCs it replaces checked theirs: a policy body by its hash; anything else
// only if an allocation placed on the caller's node declares it, with the KV
// path derived by the manager, never taken from the request.
rpc FetchDatapathMaterial (FetchDatapathMaterialRequest) returns (FetchDatapathMaterialReply);

message MaterialRef {
  string kind = 1;          // policy | port_cert | client_ca | upstream_ca | credential | plugin
  int64 allocation_id = 2;  // 0 for a policy body
  string key = 3;           // policy hash, port label, or upstream key + header
  uint64 have_version = 4;  // the version the node holds; unchanged refs return nothing
}
```

`IssueNodeCertificate` stays as it is, because it signs a CSR rather than
fetching anything.

- **The netd boundary shrinks** to `NetdWants { material: Vec<MaterialRef> }`
  and `NetdInput::Material(Vec<Material>)`, which is also the message set the
  `legion netd` process split will carry.
- **On the manager, a `MaterialProvider` trait** resolves one kind of
  source: `kv`, `identity`, and `policy` at first. A future `vault` provider
  is one implementation plus one `Source` variant. It needs no RPC, no new
  caller class, and no netd change.
- **Values stay in memory only**, as today. Nothing fetched is written to
  `netd_state.json` or logged.
- **B3's signing host function takes a material ref, never a value**
  (calls entry 12).

### 6.A Kernel classifier: decide every flow at `connect()`

One fixed set of Legion-owned eBPF programs is attached to each governed
allocation's cgroup leaf. The policy is data in maps, not code. Users write
the same rules they write today.

```
task: connect(151.101.0.223:443)
  │
  ▼  cgroup/connect4, attached to legion/share/legion-88213
  ├─ host guard ───────────────────────► EPERM           (final, as today)
  ├─ compiled chain (maps) ────────────► EPERM | pass | redirect
  │
  ├─ pass ─────► the socket connects directly; per-allocation counters only
  └─ redirect ─► netd capture listener
                   │  SNI/Host peek, the connect stage of the pipeline (S1)
                   │  HTTP mode: rules → pipeline stages (S1, B) → credentials
                   ▼
                 origin
```

#### 6.A.1 Hooks

| Hook (first kernel) | Fires when | Legion uses it to |
| --- | --- | --- |
| `cgroup/connect4`, `connect6` (4.17) | a task socket calls `connect()` (TCP, connected UDP) | deny with `EPERM`, pass, or rewrite the destination to netd's capture listener |
| `cgroup/sendmsg4`, `sendmsg6` (4.18) | unconnected UDP `sendto()` | send DNS to the allocation's resolver. Deny other UDP per policy |
| `cgroup/recvmsg4`, `recvmsg6` (5.2) | the task reads a UDP reply | make a redirected DNS reply appear to come from the nameserver the task asked |
| `sockops` (4.13) | TCP state changes | key the original-destination map by the socket's bound source address and port. Collect RTT samples |
| `cgroup/sock_create` (4.10) | `socket()` | refuse `SOCK_RAW` for governed tasks |
| `cgroup_skb/egress`, `ingress` (4.10) | every packet | per-allocation byte counters for pass flows |
| BPF ring buffer (5.8) | — | denial events to netd, which folds them into `net.denied` exactly as today |

The kernel floor is a phase-A0 question (§11). The probe checks for
features, not version numbers, because RHEL-family kernels backport.

#### 6.A.2 The policy is compiled to maps

`EffectivePolicy` is compiled per allocation into:

- an LPM trie of CIDR rules → (verdict, rule index);
- a hash of (address, port range) → (verdict, rule index), filled with the
  addresses **learned from DNS** (§6.A.4);
- the fallthrough, the `gated` flag, and a "needs netd" flag (§6.A.3);
- interned label and reason ids, so an event carries `policy/sandbox#2` and
  its reason text, not a number.

Compiling a chain is a pure function. A policy swap replaces the
allocation's map entries atomically, on the same heartbeat cadence as
today, so there is still no restart and no rollout.

#### 6.A.3 Three outcomes, and what forces a redirect

A flow is **passed** when its verdict is allow and nothing in userspace
needs it. It is **redirected** when any of these is true for the
destination:

- the chain is `protocol = "http"` and the port is plaintext HTTP (the
  request head must be read);
- the flow's pipeline (S1) has a stage later than `connect`, or a
  `connect`-stage hop placed in netd or outside it;
- a hostname rule matches but the address was not learned through the
  resolver (§6.A.4: the SNI or Host check);
- it is a declared upstream VIP. Upstreams always go through netd: kernel
  balancing is not in this plan (§6.F).

Everything else is passed or denied in the kernel. A redirected flow costs
what a governed flow costs today, minus the `CONNECT` exchange, plus one
map lookup per new connection.

**Gated tasks never pass.** Their namespace has no route out, and that is
the boundary. On a gated task, A only makes capture **transparent**: the
connect hook rewrites the destination to the in-namespace capture listener,
so a tool that ignores `HTTPS_PROXY` still gets out under policy. The proxy
variables stay set. A client that uses them gives the datapath a hostname
directly, which is the better source of attribution.

#### 6.A.4 Hostnames without reading packets

The kernel sees addresses, and Legion's rules name hosts. The bridge is the
resolver Legion already runs per allocation, which already judges every
name ([dns.rs:431](../../src/worker/netd/dns.rs)):

1. **All DNS is captured.** `sendmsg4/6` and `connect4/6` rewrite port-53
   traffic from governed tasks to the allocation's resolver, whatever
   `resolv.conf` says. This fixes a gap that exists today: a host-mode
   `exec` task keeps the host's `resolv.conf`.
2. **An allowed answer teaches the kernel.** When the resolver answers a
   name the chain allows, it writes the addresses into that allocation's
   learned-address map with the rule index and the record's TTL, raised to
   a floor so a client that caches longer than the TTL isn't cut off
   mid-use.
3. **An unlearned address falls back to a peek.** A connection to an
   address the resolver never returned (a cached answer, `/etc/hosts`, a
   hard-coded IP), under a chain with hostname rules, is redirected. netd
   reads the TLS SNI or the HTTP `Host`, resolves that name itself, and
   allows the flow only if the dialed address is one of that name's
   addresses. Then it splices. A client that lies in its SNI does not get
   through by naming an allowed host.

DNS over HTTPS bypasses step 2. Its addresses are unlearned, so step 3
judges the DoH server by its own name, which the policy has to allow.
Fronting through a CDN address that an allowed name and a denied name
share is still possible. The same is true of any egress control based on
SNI or address, Cilium's included, and the public page will say so.

#### 6.A.5 Per runtime

| Runtime and mode | Attach point | Result |
| --- | --- | --- |
| `exec` and `container`, `host` mode | cgroup hooks | **Enforced**, where it was cooperative. Pass, deny, or redirect |
| `exec` and `container`, `gated` mode | cgroup hooks, with the namespace as the backstop | Transparent capture. Every allowed flow still goes through netd |
| `sandbox`, gated (vsock) | none: the guest's sockets live in the guest kernel | Unchanged. netd judges each vsock stream, and B's plugins apply |
| `sandbox`, TAP-wired | none in this plan (`tc` enforcement is on-demand, §6.F) | Unchanged |
| macOS | none | Unchanged |

#### 6.A.6 What it costs, and what it saves

These are **estimates**. Phase A0 measures them in `net_bench` before
anything ships:

| Flow | Today | With A |
| --- | --- | --- |
| Allowed L4 flow, host mode, nothing reads it | not governed (cooperative), or one hop through an upstream | **no hop**: a few map lookups at `connect()`, sub-microsecond (estimate) |
| Allowed L4 flow, gated | `CONNECT` round trip, then one hop (45–80 µs per new connection, 4–7 µs per round trip) | the same hop, without the `CONNECT` exchange |
| Denied flow, host mode | not governed | `EPERM` from `connect()`, nothing sent |
| Redirected flow | — | one hop, plus one map lookup per new connection |
| Worker restart | every flow drops | pass flows are untouched, and the kernel keeps enforcing the last policy (fail-static) |

Pass flows do not reach netd's ledgers, so everything that reads those
ledgers reads kernel counters instead:

- the service graph: `connect4` counts per (allocation, rule, learned name);
- idle detection: `cgroup_skb` bytes per allocation;
- `legion_net_*` metrics.

The L4 access log is unchanged, because L4 connections are not logged one
by one today either.

#### 6.A.7 Lifecycle

- **Probe at start.** The node attribute becomes
  `netKernel = active (ebpf: cgroup sock_addr, sockops, ringbuf)` or
  `unavailable (<the missing feature>)`. A node without it behaves exactly
  as today, including the `nftables` guard.
- **Pinned in bpffs** under `/sys/fs/bpf/legion/`, with the links per
  allocation. A restarting worker re-adopts the pins, as it replays
  `netd_state.json`. A clean shutdown removes them, as it deletes the
  `nftables` table today.
- **Replaces the `nftables` guard** on capable nodes (phase A1): the same
  classes, the same `EPERM`, the same `net.denied` events, one mechanism,
  and no `nft` binary.

#### 6.A.8 Limits, stated up front

- A root `exec` task with `CAP_SYS_ADMIN` or `CAP_BPF` can detach programs
  from its cgroup. This is the same hole as the `nftables` guard has today.
- `AF_PACKET` sockets skip the IP stack. Governed tasks must not hold
  `CAP_NET_RAW`, which also keeps raw IP from slipping past `sock_create`.
- IPv6 has to reach parity in the same phase as IPv4, not after.
- Coverage of `io_uring` connect, TCP Fast Open, and `sendmmsg` is a
  phase-A0 test, not an assumption.

**Surface.** No new job fields. What changes is what `show`, `doctor`, and
`node info` report:

```console
$ legion net policy show batch-etl
egress  protocol tcp  ·  enforced in the kernel on 12/12 nodes
  #0 policy/no-smtp#0     deny port 25,465,587      kernel
  #1 policy/batch#1       allow *.pypi.org          kernel (learned: 6 addresses)
  #2 task#0               allow 10.0.0.0/8:5432     kernel
  fallthrough deny
  allowed flows: connect directly (no proxy)

$ legion node info wkr-1
Host guard     active (ebpf)
Net kernel     active (ebpf: cgroup sock_addr, sockops, ringbuf)
```

Inside the task:

```console
$ nc mail.example.com 25
nc: connect to mail.example.com port 25 (tcp) failed: Operation not permitted
```

### 6.B In-process plugins: WASM on the proxy-wasm ABI

This is the pluggable layer for anything that reads a request. A plugin is
a WASM module that runs inside netd. To the rest of the datapath it is one
more `Handler` (S1): `HandlerKind::Plugin`, placement `netd (wasm)`, at the
stages its object declares, under a memory cap, a fuel or epoch deadline,
and the hop's `onError`.

**Why WASM and not the other options.** A webhook costs a round trip and
can only return a verdict. A `proxy` hop costs a second proxy. `cdylib`
handlers stay rejected (network-handlers calls entry 4: a panic in native
code takes down every tenant's datapath). WASM gives a crash boundary
(traps are caught), a resource boundary (fuel and memory), and any
language, with no hop. **Why proxy-wasm** (calls entry 11): a host ABI with
existing SDKs and a body of existing filters and examples, instead of a
Legion ABI nobody has written code for yet.

**Stages** are S1's `Stage`, and map to the proxy-wasm callbacks:

| Stage | Callback | Sees | Can |
| --- | --- | --- | --- |
| `connect` | `on_new_connection` | the flow's facts | deny, or tag the flow for later stages |
| `request` | `on_http_request_headers` | method, path, headers | deny (a local response), rewrite the path, add, set, or remove headers |
| `body` | `on_http_request_body` | the body, streamed in chunks or buffered up to `maxBytes` | deny, rewrite |
| `response` | `on_http_response_headers`, `on_http_response_body` | status, headers, streamed body (SSE included) | rewrite, count, replace the response |
| `done` | `on_log` | the whole exchange | metrics, custom log lines |

A `connect`-stage plugin forces its flows through netd. On a node with A,
that means a redirect, and `show` says so.

**Facts are properties.** `legion.allocation`, `legion.job`, `legion.task`,
`legion.node`, `legion.identity`, `legion.labels.<k>`, `legion.destination`,
`legion.upstream`, and `legion.direction` are read with the SDK's
`get_property`. `legion.identity` uses S4's identity string.

**What a plugin cannot reach.**
- **Credentials.** Injection runs after the plugins, as it already runs
  after `authz`. A later phase adds a host function that *signs* with a
  declared credential, passed as an S5 material ref, without the module ever
  holding the key (calls entry 12), so SigV4 and HMAC request signing become
  possible.
- **Sockets and the filesystem.** There are no WASI sockets and no
  filesystem. `dispatch_http_call` comes in a later phase, and only to
  targets declared on the plugin object, which the egress policy then
  judges.
- **Other tenants.** Shared data (`get_shared_data`) is scoped to
  (plugin, job) for a plugin that a job references, and to the plugin for
  one a policy object applies. A job's plugin never sees another job's
  traffic or counters.

**Easy to write.** Here is a complete plugin, written against the
proxy-wasm Rust SDK with no Legion crate:

```rust
use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_http_context(|_, _| -> Box<dyn HttpContext> { Box::new(ReadOnlyFreeTier) });
}}

struct ReadOnlyFreeTier;
impl Context for ReadOnlyFreeTier {}
impl HttpContext for ReadOnlyFreeTier {
    fn on_http_request_headers(&mut self, _: usize, _: bool) -> Action {
        let tier = self.get_property(vec!["legion", "labels", "tier"]);
        let method = self.get_http_request_header(":method");
        if tier.as_deref() == Some(b"free".as_slice()) && method.as_deref() != Some("GET") {
            self.send_http_response(403, vec![("legion-denied-reason", "the free tier is read-only")], None);
            return Action::Pause;
        }
        Action::Continue
    }
}
```

```console
$ legion net plugin new read-only-free-tier --lang rust     # a scaffold like the one above
$ cargo build --release --target wasm32-wasip1
$ legion net plugin test ./target/wasm32-wasip1/release/read_only_free_tier.wasm \
    --request 'POST https://api.github.com/repos/x/issues' --label tier=free
✗ request: 403 from plugin read-only-free-tier — the free tier is read-only   (41 µs, 1.2M fuel)
$ legion net plugin apply -f read-only-free-tier.ncl --dry-run
```

**Failure.** A trap, exhausted fuel, a passed deadline, or a module that
fails to instantiate is a handler error and follows `onError`, which
defaults to `deny`. The task gets the ordinary denial shape:

```http
HTTP/1.1 503 Service Unavailable
Legion-Denied-By: plugin/llm-token-meter
Legion-Denied-Reason: plugin llm-token-meter exceeded its 2ms deadline
```

Per-plugin health (calls, errors, last error, p99 time) is reported on the
heartbeat beside `NetHandlerStatus`, so `net doctor` and the UI show it.

**Cost.** One engine per node and one pre-instantiated module per
(plugin, scope), with a per-request context. The expected cost of a
header-stage plugin is single-digit microseconds. A no-op module's row in
`net_bench http_rr` is the acceptance number. The runtime choice
(`wasmtime`, which compiles ahead of time and adds binary size, against
`wasmi`, a smaller interpreter) is open question 3.

#### 6.B.1 Plugin objects

A plugin is stored like a policy object (in the replicated store, applied
with the CLI) and referenced by name. What it is, which stages it runs at,
and who may use it live on the object, not in the reference (calls
entry 16):

```nickel
# configs/netplugins/llm-token-meter.ncl
{
    name = "llm-token-meter",
    module = {
        source = "oci://ghcr.io/acme/llm-token-meter@sha256:9f86d0…",   # or https:// + checksum
    },
    stages = ["response", "done"],
    usableBy = "policies",                           # policies | jobs (jobs from B2)
    limits = { memory = "16Mi", deadline = "2ms" },
    configSchema = {                                 # JSON Schema; every reference's config is checked at submit
        type = "object",
        required = ["budget"],
        properties = { budget = { type = "integer" }, per = { type = "string" } },
    },
}
```

The proposal's `kind = "wasm" | "ebpf"` field is gone, because operator eBPF
programs are not in this plan (§6.F). If they come back, `kind` comes back
with them, defaulting to `wasm`.

Modules are fetched by the artifact fetchers, checked against the digest,
and kept in the node content store, so a thousand allocations that use one
plugin cost one fetch. The digest reaches the node as an S5 material ref.

```console
legion net plugin new <name> --lang rust                    # scaffold
legion net plugin test <module|name> --request 'METHOD URL' [--body @f] [--label k=v] [--to dest]
legion net plugin apply -f <file.ncl> [--dry-run]
legion net plugin list | describe <name> | status <name> | delete <name>
```

Every verb takes `--json`. `test` and `apply --dry-run` are read-only. The
MCP tools are `legion_net_plugin_test`, `legion_net_plugin_list`, and
`legion_net_plugin_describe` (read-only), and `legion_net_plugin_apply`
(mutating). The RPCs are `UpsertNetPlugin` (with an `idempotency_key`),
`GetNetPlugin`, `ListNetPlugins`, `DeleteNetPlugin`, and
`GetNetPluginStatus`.

The ACLs:
- `net:read` to see plugins.
- `net:write`, scoped by pools, to apply a plugin that is
  `usableBy = "policies"`.
- Unscoped `net:write` for `usableBy = "jobs"`. A plugin that every job may
  reference is cluster-wide by nature.

### 6.F Considered and not proposed, and cut to on-demand

**Cut from the phases, kept as on-demand TODO items** (calls entry 20). Each
was in the #49 proposal. None is needed by any scenario in §3, and each adds
surface or a moving part:

| Item | What it was | Why it waits |
| --- | --- | --- |
| Operator eBPF programs, with an SDK and `legion net plugin build` (the proposal's C) | Custom connect-time logic in the kernel, from policy objects only | A's compiled rules plus B's `connect`-stage plugins cover every scenario. It would add a second plugin kind, a pinned build toolchain, and the riskiest privilege in the system. Build it when an operator brings a connect-time need neither covers |
| File-descriptor hand-off for node-local `proxy` targets, and `authz` to a sidecar port (the proposal's D) | `{ proxy = { port = "filter", handoff = true } }`; `{ authz = { port = "policy" } }` | A new knob and a second hand-off protocol for a cost (one splice) nobody has reported. Do it when a `proxy` user asks |
| Kernel balancing for plain upstreams (the proposal's A5) | Rewrite the VIP to the picked endpoint at `connect()` | Transport RTTs instead of request latency for `least_latency`, and a second balancing implementation. Revisit once A2's pass path has run in production |
| `tc` enforcement for TAP-wired sandboxes (the proposal's A6) | The compiled maps applied per packet on the TAP device | Gated sandboxes use vsock, which netd already judges. TAP-wired sandboxes are the uncommon case |

**Considered and rejected:**

| Option | Why not |
| --- | --- |
| XDP, or `tc` on the node's NIC | The wrong layer for task egress. That traffic is loopback, vsock, or a namespace with no NIC, and there is no socket identity at that point |
| `sockmap` / `sk_msg` proxying between sockets in the kernel | A second byte-moving engine to debug. `splice` already puts the forwarder at about 80% of direct bulk throughput. Revisit only if `net_bench` shows the per-round-trip wakeup dominating redirected flows |
| Job authors loading eBPF | A kernel attack surface exposed to tenants. Calls entry 2 |
| A policy expression language compiled to eBPF | A second policy language, and two evaluators to keep equal. Rules plus selectors cover it. Calls entry 2 |
| Envoy as the L7 tier (Cilium's model) | Two datapaths. Service-mesh calls entry 3 already rejected a waypoint |
| `cdylib` handlers | Network-handlers calls entry 4 stands |
| TUN plus a userspace TCP stack for transparent capture | Per-packet userspace work, and heavier than a connect hook. A supersedes the deferred phase 7 capture on capable nodes. Calls entry 18 |
| eBPF inside sandbox guests | The guest is untrusted, so enforcement stays on the host side of the vsock |
| A WASM component-model ABI (a Legion WIT world) | Modern and typed, but no filters exist for it yet. Calls entry 11 records what would change the call |
| Keep the flat handler struct and add `plugin` as one more `handler` value | Calls entry 13 |
| Per-path handler dispatch, with the phase-0 gap patched | Calls entry 3 |
| Protocol detection on upstreams (Linkerd's model) | Calls entry 6 |
| An HTTP chain forcing HTTP parsing on every upstream (today's rule) | Calls entry 6 |
| A `svc://db` scheme, or requiring `db.svc.legion`, for catalog upstreams | Calls entry 14 |
| `tls = "mesh"` shorthand on chains instead of `service.mesh` | Calls entry 8 |
| Dropping `service.allow` for ingress identity rules | Calls entry 7 |
| One RPC per material kind, with a provider added per RPC | Calls entry 10 |

---

## 7. Proto, struct, and config changes (sketch)

**S1**
- `HandlerConfig` becomes `HopConfig { kind: HandlerKind, timeout, on_error }`,
  with `HandlerKind` = `Authz`, `Proxy`, `Dlp`, and (B1) `Plugin`. `Log` is
  removed, and so is `ChainConfig.on_error`. The policy hash format changes.
- `src/worker/netd/handlers.rs` becomes a module directory: one file per
  kind, the `Handler` trait, `HANDLERS`, and `Pipeline`.
- `NetHandlerStatus.handler` carries the registry name. Its shape is
  otherwise unchanged.

**S2**
- `UpstreamConfig { to, name, port, balance, http, handlers, credentials, ca }`.
  `service` and `local_port` are removed.
- `AllocUpstream.service` becomes `key`, and `local_port` becomes optional:
  it is set on macOS, when the node has no VIP block, or when `port` is
  pinned. The rename rides a store migration
  ([storage/migrations.rs](../../src/storage/migrations.rs)).
- `UpstreamHttpConfig` loses `outlier`. `RetryConfig` is unchanged.
- `BALANCERS` loses `random`. `RoundRobin` and `LeastConn` seed their cursor
  at a random offset.
- `env_pairs` and the template address share one function.

**S3**
- `ServiceConfig.mesh: bool`, and `NetPolicyObject.mesh: Option<bool>`.
- `PortConfig.tls: Option<PortTls { cert: Source, clients: Option<ClientAuth { ca: Source }> }>`.
- `ChainConfig.tls` and `TlsConfig` are removed. `AllocNetwork.ingress_tls`
  keeps its role (`mesh`, `byo`, or absent), resolved from `service.mesh`
  and `ports[].tls`.
- The `bridge`/`cni/*` branch in `validate_network_mode` is removed.

**S4**
- The `ServiceConfig.allow` grammar widens. Bare entries keep their meaning.
- `allow_permits` takes an identity rather than a job id.
- The compiled ingress rule carries the source `service/allow`. On the wire,
  `ServiceSummary.allow` is unchanged.

**S5**
- `Source` is shared by S3's TLS fields, the upstream's `ca`, and
  `credentials[].from`.
- `FetchDatapathMaterial` replaces three RPCs, with full proto comments, an
  entry in `tests/acl_endpoints.rs`, and the worker-plane caller class in
  `tests/worker_plane_classes.rs`.
- `NetdWants` and `NetdInput` shrink to one material variant each.

**A and B**
- **A new `NetPlugin` record** (name, module source and digest, stages,
  usableBy, limits, configSchema), stored like `net_policy`, with the five
  RPCs in §6.B.1. Each has full proto comments and an entry in
  `tests/acl_endpoints.rs`.
- **`NetAllocStatus` gains:**
  - `plugins` (health per plugin, as `handlers` reports today);
  - `kernel` (whether the allocation is attached, its map generation, and
    its pass, redirect, and deny counters).
- **The node attribute `netKernel`.** `hostGuard` reads `active (ebpf)` or
  `active (nftables)`.
- **New dependencies.** `aya` (A, Linux only, pure Rust at run time), a WASM
  runtime (B), and `proxy-wasm` (dev only, for the conformance modules).

**Every track:** `configs/jobs/maximal.ncl`, every `configs/jobs/*.ncl` and
`configs/policies/*.ncl` that uses a changed shape, and the public pages
(`docs/network-handlers.md`, `docs/service-discovery.md`,
`docs/agent-readiness.md`) change in the PR that changes the shape. MCP tool
schemas that echo these shapes (`legion_net_policy_show`, `_test`, `_apply`)
follow.

## 8. Phases

Every phase ships on its own and is useful on its own. Phase 0 comes first.
S1 comes before B1, and before A2's redirect rule reads stages. Everything
else is independent.

**Phase 0: bugs (no plan needed; ships first; calls entry 19)**

| Phase | Ships | Value on its own |
| --- | --- | --- |
| **0** | Tests that a chain-level `dlp` and `log`, inline and on a policy object, run on an HTTP-mode upstream, the gated proxy's absolute-URI path, and HTTP-mode ingress. The fix passes the effective chain's handlers to `HttpSettings` beside the upstream's. Hops without their own `onError` take the chain's, so the datapath does what `show` prints and the public page documents | A policy that says it inspects bodies does, and the shown failure mode is the real one |

**Track S: surface**

| Phase | Ships | Value on its own |
| --- | --- | --- |
| **S1** | Keyed handler variants, the `Handler` trait, `HANDLERS`, one compiled `Pipeline` on every path, stage checks at submit, and placement in `show`/`doctor` (`netd` and `external` until A and B add theirs). Removes `log`, chain `onError`, and `proxy.node` | One place to add a handler. Misplaced fields fail at evaluation. The phase-0 class of bug cannot recur |
| **S2** | One `to`, the scheme as the protocol, fail-closed HTTP chains over `tcp` upstreams, one address per upstream, VIP-only binding on Linux. Removes `outlier` and `random` | Scenario 6. The env var and the template agree. No dynamic-range port per upstream on Linux |
| **S3 + S4** | `service.mesh`, `ports[].tls`, upstream `ca`, and chains without `tls`; `service.allow` with one grammar, compiled to the first ingress rule on mesh services, plus the `service doctor` notice. Removes the reserved modes. One phase, because both reshape the receiving side and S4 replaces the `requireIdentity` that S3 removes | Scenario 5. The mesh is one switch, and callers are one list |
| **S5** | `Source`, `FetchDatapathMaterial`, `MaterialProvider`, and `tls.clients.ca = { kv }` | Closes the KV client-CA TODO. A new secret store is one provider |

**Track A: kernel**

| Phase | Ships | Value on its own |
| --- | --- | --- |
| **A0** | The feature probe, the `netKernel` attribute, and `net_bench` groups `kernel_pass_conn_rr`, `kernel_redirect_conn_rr`, `kernel_connect_cost`. A findings doc on coverage (`io_uring`, TFO, IPv6, connected and unconnected UDP) and restart survival | Measured numbers before any design is committed |
| **A1** | The host guard in eBPF (`connect4/6`, `sock_create`), with the same semantics and events as the `nftables` table, and pinned links | One mechanism, no `nft`, per-allocation counters. Proves the loader and the lifecycle on known behavior |
| **A2** | Kernel-enforced L4 egress for host-mode tasks: compiled maps, DNS capture with learned addresses, ring-buffer denials, the pass path, and kernel counters feeding the graph, idle detection, and metrics | Host-mode policy is real. Allowed flows cross no proxy |
| **A3** | Transparent capture: redirect into netd from host mode (for flows that need it) and from gated namespaces, plus the SNI/Host peek | Tools that ignore proxy variables work in gated tasks. Unlearned addresses are judged by name |

**Track B: plugins (needs S1)**

| Phase | Ships | Value on its own |
| --- | --- | --- |
| **B1** | Plugin objects (§6.B.1: CLI, RPCs, MCP), and WASM at the `request` and `done` stages, from policy objects only | Custom per-request logic with no second proxy |
| **B2** | The `body` and `response` stages, streaming bodies (SSE), and `usableBy = "jobs"` with shared data scoped per job | Scenario 1, and job-author plugins |
| **B3** | Credential signing host functions over S5 material refs, and `dispatch_http_call` to declared targets | SigV4 and HMAC signing, and cross-node budgets through a service |

## 9. Testing

- **Phase 0.** Regression tests on each HTTP path named in §8, for a chain
  `dlp`, a chain `log`, and a chain `onError = "allow"` with a failing hop.
- **S1: registry parity.** A test fails the build when a `HANDLERS` row has
  no Nickel contract, no `maximal.ncl` entry, or no row in the public
  handler table, in the way `BALANCERS` is checked by `is_known_balance`.
- **S1: pipeline coverage.** One table-driven test runs a hop of every kind
  through every path (L4 upstream, HTTP upstream, gated `CONNECT`, gated
  absolute URI, SOCKS5, ingress L4, ingress HTTP) and asserts it ran at
  every stage that path can see, or was refused at submit.
- **S2.**
  - Table-driven parse tests for `to` (catalog against external, schemes,
    dotless hosts with and without a port).
  - A test that `env_pairs` and `{{ service }}` agree on Linux, macOS, and a
    node with no VIP block left.
  - A Postgres-greeting stream under an HTTP `require` chain: allowed by a
    host rule, and denied with the documented reason when the first rule
    needs a path.
  - HTTP-mode outlier ejection still happens with no `outlier` block.
  - Many fresh `round_robin` listeners over one endpoint list spread their
    first picks across the endpoints instead of all starting at index 0.
- **S3 + S4.**
  - Equivalence: `mesh = true` produces the same listener, catalog flag, and
    consumer behavior that the three-field form did.
  - `allow` is enforced at the receiving node on a mesh service. A forged
    identity from a non-node peer is refused at the handshake (the existing
    test).
  - The control-plane-only notice in `service doctor` without the mesh.
- **S5.**
  - The ACL suite: `FetchDatapathMaterial` refuses a ref that no allocation
    on the caller's node declares, for every kind.
  - Fetched values never appear in `netd_state.json` or a log line.
  - A provider-registry test.
- **The compiler and `evaluate_connect` must agree.** A property test
  generates random chains and facts and checks that evaluating the compiled
  maps in a Rust model of the kernel lookup gives the same verdict and rule
  as `evaluate_connect`. This is how "the node runs the same function as
  `policy show`" stays true when part of the evaluation runs in the kernel.
- **Kernel tests** (root, a capable kernel) run in a dedicated CI job with
  `sudo`, marked like the existing Linux-only tests:
  - attach and detach;
  - `EPERM` on a deny;
  - pass with no netd involvement (netd stopped mid-test, and the flow
    survives);
  - redirect with the original destination recovered;
  - DNS capture and learned addresses expiring;
  - IPv6;
  - re-adopting pins after a restart.
- **WASM conformance.** The proxy-wasm Rust SDK's example filters run
  unmodified. Further tests cover fuel and deadline exhaustion, traps, a
  module that fails to instantiate, isolation of shared data between jobs,
  and the denial shape.
- **Benchmarks.** `net_bench` before and after each phase that touches the
  per-connection or per-request path: S1 (the pipeline must be within noise
  of today on `conn_rr` and `http_rr`), S2, A0 onward, and B1. Each PR
  carries both numbers, as the datapath plan did.
- **Existing suites run unchanged** on macOS and on Linux nodes without
  `netKernel`.

## 10. Rollout and what breaks

Legion has no installed base to protect (AGENTS.md), so each of these is
a decision, listed for the PR descriptions. There are no aliases for any old
form.

- **Handler syntax changes** to keyed variants. `log` and chain-level
  `onError` are removed, and `proxy.node` becomes `service` + `local`. Every
  chain with handlers changes in the S1 PR.
- **Upstreams change shape.** `service` and `localPort` become `to` and
  `port`. On Linux, `LEGION_UPSTREAM_<K>` changes value to the VIP address,
  `LEGION_UPSTREAM_HOST_<K>` becomes an address instead of a name, and
  `LEGION_UPSTREAM_VIP_<K>` is removed. An HTTP chain no longer parses `tcp`
  upstreams as HTTP. `outlier` and the `random` balancer are removed.
- **TLS moves out of chains.** `ingress.tls` and `egress.tls` are removed,
  in favor of `service.mesh`, `ports[].tls`, and the upstream's `ca`. A
  policy object can no longer set certificate material. It can impose
  `mesh = true`. `requireIdentity` is removed in favor of `service.allow`.
- **Three worker-plane RPCs become one.** Managers and workers must run the
  same S5 build, under the version-skew policy in
  [docs/upgrades.md](../../docs/upgrades.md).
- **Host-mode egress chains become enforced** on capable Linux nodes. A
  task that dialed past its own chain now gets `EPERM`. That is the right
  default: a policy you write should hold.
- **The `nftables` guard is replaced** on capable nodes. Nodes without the
  features keep it (open question 4).
- **Nodes below the kernel floor** behave exactly as today and say why in
  `node info` and `net doctor`.

## 11. Open questions (with leans)

1. **The kernel floor.** Lean: whatever A0's probe shows is needed. The
   expected floor is around 5.15, the oldest current LTS distribution
   kernels. Features are probed, not versions.
2. **The eBPF build toolchain.** `aya-ebpf` (Rust, needs nightly and
   `bpf-linker`), C with `clang`, or checked-in objects. Lean: `aya-ebpf`
   sources, with objects built in CI and committed, plus a CI job that
   rebuilds and diffs them. `cargo build` then still needs only `protoc`.
3. **The WASM runtime.** `wasmtime`: fast, but measure what it adds to the
   26 MB `dist` binary. `wasmi`: small, interpreted. Lean: `wasmtime`,
   decided on B1's numbers. Fall back to `wasmi` if the size cost is out of
   proportion.
4. **Keep the `nftables` fallback?** Lean: yes, until the floor is settled.
   It is a platform fallback, not a compatibility shim. Drop it once every
   supported distribution passes the probe.
5. **A resolver address for every governed allocation.** VIP blocks are
   handed out per allocation today, only to those with upstreams. A2 needs
   a resolver for every enforced allocation. Lean: give each a block. If
   the address space binds first, use a node resolver that attributes
   queries through the connect-time map.
6. **Should host mode become the high-throughput recommendation for
   trusted containers** on enforced nodes? Lean: document the trade-off,
   and leave `auto` alone. Gated is a namespace boundary, and enforced host
   mode is a cgroup-hook boundary.
7. **Job-author plugins in B1 or B2?** Lean: B2, after per-job scoping and
   the limits have run in production on policy-applied plugins.
8. **May credentials come from a `file` on the node?** Lean: no.
   Credentials stay cluster-held (`kv`, `identity`), so a node's disk never
   becomes a secret store, and every credential's access is checked at
   submit against `kv:read`.
9. **Is fixed outlier ejection right for every service?** Five consecutive
   5xx for 30 s is today's default. Lean: yes. A service that needs
   different numbers is evidence for bringing back one knob, not the
   block.

## 12. Acceptance (per phase)

- **0.** A policy object with `dlp` blocks a matching body on all three HTTP
  paths. A chain `log` writes access lines. A chain `onError = "allow"` fails
  open on a dead webhook, as `show` prints.
- **S1.** A test-only handler kind is added by touching one file plus its
  registry row, contract, and docs row, and the parity test fails if any is
  missing. A misplaced `url` on a `proxy` entry fails evaluation, naming
  `proxy`. `net_bench conn_rr` and `http_rr` are within noise of the base
  branch.
- **S2.** Scenario 6 works end to end. `LEGION_UPSTREAM_<K>` and
  `{{ service }}` agree. On Linux, `net doctor` shows VIP listeners and no
  dynamic-range port per upstream.
- **S3 + S4.** Scenario 5 works with the two-line form. `legion net policy
  show api` prints the `service/allow` rule. A non-mesh service's `allow`
  shows the control-plane-only notice in `service doctor`.
- **S5.** A mutual port verifies clients against a CA held in KV.
  `tests/acl_endpoints.rs` covers `FetchDatapathMaterial`, and the three
  replaced RPCs are gone from `proto/legion.proto`.
- **A0.** `cargo bench --bench net_bench -- kernel_` runs. The findings doc
  states the measured cost of a pass, a redirect, and a deny, and the
  coverage results.
- **A1.** On a capable node, `node info` shows `Host guard active (ebpf)`,
  the existing kernel-guard tests pass against it, and `nft list tables`
  shows no `legion_host_guard`.
- **A2.** Scenario 2 works end to end. `kernel_pass_conn_rr` is within
  noise of direct. A flow survives a `kill -9` of the worker.
- **A3.** Scenario 3 works with `HTTPS_PROXY` unset. A spoofed SNI to an
  unlearned address is denied with a reason naming the mismatch.
- **B1.** The example plugin in §6.B denies through a policy object.
  `net_bench http_rr` with a no-op plugin is within single-digit
  microseconds of without one. A trapping plugin produces the documented
  `503`, and its status shows the error.
- **B2.** Scenario 1 works against a streamed response. A job's plugin
  cannot read another job's shared data (there is a test).
