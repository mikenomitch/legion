# Proposal: Egress Plugins — a Kernel Fast Path and Pluggable Stages

> **Judgement calls:** [`calls/egress-plugins-proposal_calls.md`](../calls/egress-plugins-proposal_calls.md)

> **Status: PROPOSAL.** Nothing here is built. It answers one question:
> *can egress proxy code be both as cheap as possible and pluggable, perhaps
> with eBPF plugins that are easy to write?* It sets out five proposals
> (A–E), recommends **A + B + E**, calls D a cheap add-on, and puts C last.
> Each proposal is phased so it can graduate into its own plan.
>
> It builds on, and does not replace, the shipped
> [service-mesh plan](service-mesh-plan.md),
> [network-handlers plan](network-handlers-plan.md), and
> [datapath-performance plan](datapath-performance-plan.md). Proposal B is
> the handlers plan's deferred phase 8 (WASM handlers), reshaped.
> Proposal A reopens the datapath plan's deferred "connect-time rewriting"
> (§6.7) with an answer to its objection. That objection was losing
> per-connection accounting and policy.

---

## 0. The short version

The cheapest proxy is the one a flow never enters. Today every flow that
Legion governs enters the worker's userspace forwarder, even when nothing
needs to read it. Custom logic then costs either a webhook round trip
(`authz`) or a second full proxy hop (`proxy`).

The proposal splits the problem along the line where eBPF stops being the
right tool:

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

| | Proposal | What it buys | Who writes the plugin code | Runs on | Lean |
| --- | --- | --- | --- | --- | --- |
| **A** | Kernel classifier: policy compiled to eBPF maps | Allowed L4 flows skip the proxy entirely. Host-mode policy becomes enforced, not cooperative. Capture becomes transparent. Pass flows survive a worker restart | Nobody: the rules you already write compile to maps | Linux, `exec` and `container` | **Do it.** This is the efficiency win |
| **B** | WASM plugins on the proxy-wasm ABI | Custom per-request logic in-process, at a cost of microseconds rather than an RPC or a second hop | Job authors and operators, in Rust, C++, or any language with a proxy-wasm SDK | Everywhere netd runs: sandboxes and macOS included | **Do it.** This is the pluggability win |
| **C** | Operator eBPF programs, with an SDK | Custom connect-time logic in the kernel | Operators who want kernel-speed L4 logic | Linux, `exec` and `container` | Last, only on demand |
| **D** | Cheaper external handlers | File-descriptor hand-off removes the `proxy` hop's second splice. `authz` over a local socket | Anyone, in any language | Everywhere | Cheap; do it when a `proxy` user asks |
| **E** | Interface reshape | One handler list, plugin objects, submit-time stage checks, and "where it runs" in `show`/`doctor` | — | — | Do it with B |

A and B are independent, so they can ship in either order or in parallel.

---

## 1. Problem

What a user hits today, concretely:

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
- **Istio ambient.** A per-node L4 proxy (ztunnel) plus an optional per-
  service L7 proxy (waypoint). *Take:* L7 cost only where L7 is asked for.
  *Reject:* a second proxy component (service-mesh calls entry 3 already
  made this call).
- **Nomad + Consul.** Envoy sidecars, with transparent proxy by a CNI
  plugin that installs iptables rules. Egress control is intentions between
  services. There is no plugin point in the datapath besides Envoy's own
  configuration. *Reject:* a sidecar per task.
- **Kubernetes.** `NetworkPolicy` is L3/L4 only, enforced by whatever CNI
  is installed, and has no FQDN rules in core. The extension model is
  "replace the CNI." **bpfman** treats eBPF programs as cluster objects that
  a privileged daemon loads, gated by RBAC so applications need no
  privilege ([bpfman](https://next.redhat.com/project/bpfman/)). *Take:* a
  kernel program is a cluster-scoped object that only operators may load.
- **Cloudflare `sk_lookup`.** Steers connections for any address and port
  to one listening socket without iptables, and the accepted socket keeps
  the original destination as its local address. This is the alternative
  capture mechanism, considered in calls entry 10.

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

**Security / platform admin**
- When I say "no SMTP from the free tier", I want that enforced on every
  matching task, including tasks on host networking, so a raw socket can't
  route around it.
- When a job author ships a plugin, I want it unable to see other tenants'
  traffic or our API keys, so letting authors extend the datapath doesn't
  widen what a tenant can reach.

**Cluster operator**
- When I upgrade or restart a worker, I want running tasks' connections to
  survive, so a deploy isn't an outage for long-lived streams.
- When a plugin is slow or broken, I want it contained and named, so one
  bad plugin can't take down the node's networking.

**Autonomous agent**
- When I write a plugin, I want to test it against a sample request without
  a cluster, and dry-run applying it, so I can fix it before it touches
  traffic.
- When my request is refused, I want to read which rule or plugin refused it
  and why, so I can change course instead of retrying.

*Not in scope:* the on-call responder gets nothing new beyond `net doctor`
naming where each rule runs. The incident tooling that exists already
covers them.

### User scenarios

1. **A per-user token budget on an agent platform.** Free-tier sandboxes
   call `api.openai.com` through an external `https://` upstream, which is
   already plaintext to the node. An operator applies a `llm-token-meter`
   WASM plugin to `tier = "free"`. It reads each streamed response, adds up
   the usage per `labels.user`, and returns a structured `429` once the
   budget is spent. There is no second proxy and no webhook (proposal B).
2. **Closing SMTP on a batch pool.** A policy object for pool `batch` denies
   ports 25, 465, and 587 and allows `*.pypi.org`. A host-mode `exec` task
   that runs `nc mail.example.com 25` gets `EPERM` at once, and the
   `net.denied` event names the rule. Its `pip install` connects straight to
   PyPI's CDN with no proxy in the path (proposal A).
3. **A tool that ignores `HTTPS_PROXY` in a gated container** reaches
   `github.com` because policy allows it, and is refused for `evil.example`.
   Nothing in the image was changed (proposal A, transparent capture).
4. **An agent writes a header-rewriting plugin.** It runs
   `legion net plugin new`, edits one function, runs
   `legion net plugin test` against a synthetic request, then
   `legion net plugin apply --dry-run`, then applies it (proposals B and E).

## 4. Goals and non-goals

**Goals**
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
- **Kernel code from job authors.** Only operators load eBPF (calls
  entry 2).
- **TLS interception.** It stays rejected. External upstreams remain the
  body-visible route.
- **Replacing netd** with Envoy, Pingora, or a CNI. There is one datapath.
- **HTTP/2, kTLS, and the `legion netd` process split.** These are separate
  items. Nothing here makes them harder.
- **Kernel enforcement on macOS, or inside sandbox guests.** The guest is
  untrusted, so enforcement stays on the host side of the vsock.
- **Rate limits and quotas as a rule action.** A plugin can do this
  (scenario 1). A first-class `rateLimit` action is a separate decision.

## 5. What exists today

| What | Where | Why it matters here |
| --- | --- | --- |
| The L4 forwarder and `ForwarderContext` (policy lookup, denials, credentials, handler state) | [forwarder.rs:226](../../src/worker/forwarder.rs) | The redirect target for proposal A, and the host for B |
| `splice(2)` for bulk L4, with a copy fallback | [netd/splice.rs:93](../../src/worker/netd/splice.rs) | Redirected flows keep it |
| Connect-time and request-time evaluators | `evaluate_connect` / `evaluate_request`, [netpolicy.rs:1309](../../src/structs/netpolicy.rs) | The single source of truth that the kernel maps must agree with (§9) |
| External hops: `hops_for`, `run` | [netd/handlers.rs:99](../../src/worker/netd/handlers.rs), `:150` | Where WASM plugins slot into the chain |
| HTTP mode: rules, hops, credentials, built-in `dlp`/`log` | [netd/http.rs:731](../../src/worker/netd/http.rs) (`handle`), `:272` | The request, body, and response stages B hooks |
| The gated egress proxy (`CONNECT`, absolute URI, SOCKS5) | [netd/proxy.rs](../../src/worker/netd/proxy.rs) | Keeps working; transparent capture sits beside it |
| The per-allocation resolver, which already judges names against policy | [netd/dns.rs:431](../../src/worker/netd/dns.rs) | The source of kernel FQDN policy (§6.A.4) |
| The kernel guard: one `nftables` table matching sockets by cgroup leaf | [netd/kernel_guard.rs](../../src/worker/netd/kernel_guard.rs) | Proves the per-allocation cgroup attach point. Phase A1 re-implements it in eBPF |
| Every task gets a cgroup leaf `legion/{share,reserve}/legion-<alloc>`, even with resource enforcement off | [oci_bundle.rs:1679](../../src/task_plugins/oci_bundle.rs), [resources.rs](../../src/worker/resources.rs) | The attach point for every hook in A |
| Gated namespaces: loopback plus listeners | [netd/netns.rs:58](../../src/worker/netd/netns.rs) | Stay the boundary. A adds transparency, not a replacement |
| Restart replay from `netd_state.json` | [netd/state.rs](../../src/worker/netd/state.rs) | Pinned eBPF links extend this to the kernel tier |
| Handler config is a flat bag: `url` only means something for `authz`, `service` only for `proxy` | `HandlerConfig`, [netpolicy.rs:404](../../src/structs/netpolicy.rs) | What proposal E reshapes |

---

## 6. Proposals

### 6.A Kernel classifier: decide every flow at `connect()`

One fixed set of Legion-owned eBPF programs is attached to each governed
allocation's cgroup leaf. The policy is data in maps, not code. Users write
the same rules they write today.

```
task: connect(151.101.0.223:443)
  │
  ▼  cgroup/connect4, attached to legion/share/legion-88213
  ├─ host guard ───────────────────────► EPERM           (final, as today)
  ├─ operator programs (proposal C) ───► EPERM | next | redirect
  ├─ compiled chain (maps) ────────────► EPERM | pass | redirect
  │
  ├─ pass ─────► the socket connects directly; per-allocation counters only
  └─ redirect ─► netd capture listener
                   │  SNI/Host peek, L4 handlers
                   │  HTTP mode: rules → plugins (B) → authz/proxy (D) → credentials
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
- the chain or the matched rule carries an external handler or a plugin
  with a stage later than `connect`;
- a hostname rule matches but the address was not learned through the
  resolver (§6.A.4: the SNI or Host check);
- it is a declared upstream VIP that is not eligible for kernel balancing
  (§6.A.7). That is every upstream until phase A5.

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
| `sandbox`, TAP-wired | optional later phase: `tc` on the TAP device, using `bpf_sk_assign` to steer into netd | The same compiled maps, applied per packet with a flow cache |
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

#### 6.A.7 Later: kernel balancing for plain upstreams

An upstream with no `http` block, no handlers, and no mesh TLS can be
balanced at `connect()`: the VIP is rewritten to the picked endpoint, and
`getpeername` is hooked to return the VIP. This is Cilium's socket load
balancing. The datapath plan rejected it for losing accounting and policy.
Here policy is in the same program, connections are counted per endpoint,
and `least_latency` takes TCP round-trip samples from `sockops`. Those are
transport RTTs, not request latencies, which is the trade-off. It is
eligibility-based with no knob: declaring `http` opts a flow out. This is
phase A5.

#### 6.A.8 Lifecycle

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

#### 6.A.9 Limits, stated up front

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
a WASM module that runs inside netd, at the stages it declares, under a
memory cap, a fuel or epoch deadline, and the hop's `onError`.

**Why WASM and not the other options.** A webhook costs a round trip and
can only return a verdict. A `proxy` hop costs a second proxy. `cdylib`
handlers stay rejected (network-handlers calls entry 4: a panic in native
code takes down every tenant's datapath). WASM gives a crash boundary
(traps are caught), a resource boundary (fuel and memory), and any
language, with no hop. **Why proxy-wasm** (calls entry 6): a host ABI with
existing SDKs and a body of existing filters and examples, instead of a
Legion ABI nobody has written code for yet.

**Stages**, and the proxy-wasm callbacks they map to:

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
`get_property`.

**What a plugin cannot reach.**
- **Credentials.** Injection runs after the plugins, as it already runs
  after `authz`. A later phase adds a host function that *signs* with a
  declared credential, without the module ever holding the key (calls
  entry 7), so SigV4 and HMAC request signing become possible.
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

### 6.C Operator eBPF programs: the literal "eBPF plugins"

This is for operators who want custom **connect-time** logic at kernel
speed: port-range rules, per-allocation connection-rate caps, sampling,
steering a class of flows to a handler. Legion's root program calls them in
chain order, after the host guard and before the compiled chain.

**Easy to write**, because the SDK hides the kernel. A plugin sees
Legion's own stable `Conn` (allocation, job, destination address and port,
protocol, interned labels, learned name), not kernel structs:

```rust
#![no_std]
#![no_main]
use legion_netprog::{connect, label, reason, Conn, Verdict};

#[connect]
fn no_smtp_on_free_tier(conn: &Conn) -> Verdict {
    if matches!(conn.dst_port(), 25 | 465 | 587) && conn.has(label!("tier" = "free")) {
        return Verdict::Deny(reason!("SMTP is closed on the free tier"));
    }
    Verdict::Next
}
```

- `legion net plugin build ./no-smtp` builds the BPF object in a pinned
  toolchain container, so the author installs no nightly Rust and no
  `bpf-linker`.
- `label!` and `reason!` are interned at build time. A denial event carries
  the reason text.
- `#[map]` declares per-program state, such as an LRU of counters for rate
  caps.
- Legion handles everything else: tail-call dispatch, the per-CPU scratch
  space that carries `Conn`, attaching to every allocation the referencing
  policy selects, pinning across restarts, and per-node load status with
  the verifier log on failure.

**Bounded on purpose.**
- **Policy objects only** (`usableBy = "policies"` is the only value
  allowed for `kind = "ebpf"`).
- **Applying one needs unscoped `net:write`.** Loading kernel code is a
  node-wide privilege, and verifier bugs have been privilege escalations.
- **Connect stage only.** A program cannot read a request, call out, or
  sleep. It never sees a gated sandbox's vsock flows, and it doesn't run on
  macOS.
- **Load failures follow `onError`.** A node whose kernel rejects the
  program reports it as unhealthy there, and `onError` (default `deny`)
  decides what its allocations get.

C comes last, because A's rules plus B's plugins cover every scenario in §3.
Build C when an operator brings a connect-time need that neither covers.

### 6.D Cheaper out-of-process handlers

The external tier stays the any-language, heavy-logic option. It gets
cheaper in two ways:

- **File-descriptor hand-off for node-local proxies.** A `proxy` target on
  the same node that declares support receives each connection as a file
  descriptor over a Unix socket (`SCM_RIGHTS`), with the same PROXY v2 TLV
  block and any bytes netd already read. It then owns the connection end to
  end. Today's path runs client → netd → proxy → origin. Hand-off makes it
  client → proxy → origin: one hop and one splice fewer. The proxy's own
  egress is governed by its own chain. A vsock stream is a Unix socket on
  the host, so this works for sandboxes too. The receiving side is a few
  lines in any language (`socket.recv_fds` in Python,
  `syscall.ParseUnixRights` in Go).

  ```nickel
  { proxy = { port = "filter", handoff = true } }   # sidecar gets LEGION_HANDOFF_SOCKET
  ```

- **`authz` to a node-local sidecar.** `{ authz = { port = "policy" } }`
  calls a sidecar task's port label, the way `proxy` already can, on a
  kept-alive local connection. Cache misses stop paying a network round
  trip.

### 6.E Interface reshape

AGENTS.md says to break the surface when it gives a cleaner one. Four
changes.

**1. A handler entry is a keyed variant**, like `from = { kv = … }` and
`certFrom = { kv = … }` already are. The shared settings (`timeout`,
`onError`) stay on the entry.

Before:

```nickel
handlers = [
    { handler = "authz", url = "http://policy.internal/check", cache = "30s", onError = "deny" },
    { handler = "proxy", service = "egress-filter", proxyProtocol = true },
    { handler = "dlp", config = { maxBytes = "1Mi", patterns = ["(AKIA|sk-)[A-Za-z0-9]{16,}"] } },
]
```

After:

```nickel
handlers = [
    { authz = { url = "http://policy.internal/check", cache = "30s" }, onError = "deny" },
    { proxy = { service = "egress-filter", proxyProtocol = true } },
    { dlp = { maxBytes = "1Mi", patterns = ["(AKIA|sk-)[A-Za-z0-9]{16,}"] } },
    { plugin = "llm-token-meter", config = { budget = 2000000, per = "labels.user" }, timeout = "2ms" },
]
```

Each variant gets its own Nickel contract, so a misplaced `url` fails at
evaluation with the variant named. A new kind adds one variant, not more
optional fields on a shared struct.

**2. Plugin objects.** A plugin is stored like a policy object (in the
replicated store, applied with the CLI) and referenced by name. Its kind
lives on the object, not in the reference (calls entry 9):

```nickel
# configs/netplugins/llm-token-meter.ncl
{
    name = "llm-token-meter",
    kind = "wasm",                                   # wasm | ebpf
    module = {
        source = "oci://ghcr.io/acme/llm-token-meter@sha256:9f86d0…",   # or https:// + checksum
    },
    stages = ["response", "done"],
    usableBy = "policies",                           # policies | jobs  (ebpf: policies only)
    limits = { memory = "16Mi", deadline = "2ms" },
    configSchema = {                                 # JSON Schema; every reference's config is checked at submit
        type = "object",
        required = ["budget"],
        properties = { budget = { type = "integer" }, per = { type = "string" } },
    },
}
```

Modules are fetched by the artifact fetchers, checked against the digest,
and kept in the node content store, so a thousand allocations that use one
plugin cost one fetch.

```console
legion net plugin new <name> --kind wasm|ebpf --lang rust   # scaffold
legion net plugin build <dir>                               # ebpf: pinned toolchain
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
- `net:write`, scoped by pools, to apply a `wasm` plugin that is
  `usableBy = "policies"`.
- Unscoped `net:write` for `kind = "ebpf"` and for `usableBy = "jobs"`. A
  plugin that every job may reference is cluster-wide by nature.

**3. Stages are checked at submit**, the way a path rule on a `tcp` chain
is today:

```
egress.handlers[0] plugin "llm-token-meter" runs at stage "response", but
egress.protocol is "tcp". Set protocol = "http", or put it on an upstream
whose `to` is http:// or https://.
```

```
egress.handlers[1] plugin "no-smtp" is an ebpf program, and ebpf programs
run only from policy objects. Ask an operator to apply it with a policy
that selects this job.
```

**4. Every rule and hop says where it runs.** `show`, `doctor`, and the UI
label each one `kernel`, `netd`, `netd (wasm)`, or `external`, and give the
chain's flow path (`pass`, `redirect`, or `netd`). The handlers plan holds
that `protocol` is never inferred, so that a reviewer can read the cost
straight off the policy. Placement keeps that property by being printed,
not implied.

### 6.F Considered and not proposed

| Option | Why not |
| --- | --- |
| XDP, or `tc` on the node's NIC | The wrong layer for task egress. That traffic is loopback, vsock, or a namespace with no NIC, and there is no socket identity at that point. `tc` is kept only for TAP-wired sandboxes (§6.A.5) |
| `sockmap` / `sk_msg` proxying between sockets in the kernel | A second byte-moving engine to debug. `splice` already puts the forwarder at about 80% of direct bulk throughput. Revisit only if `net_bench` shows the per-round-trip wakeup dominating redirected flows |
| Job authors loading eBPF | A kernel attack surface exposed to tenants. Calls entry 2 |
| A policy expression language compiled to eBPF | A second policy language, and two evaluators to keep equal. Rules plus selectors cover it. Calls entry 2 |
| Envoy as the L7 tier (Cilium's model) | Two datapaths. Service-mesh calls entry 3 already rejected a waypoint |
| `cdylib` handlers | Network-handlers calls entry 4 stands |
| TUN plus a userspace TCP stack for transparent capture | Per-packet userspace work, and heavier than a connect hook. A supersedes the deferred phase 7 capture on capable nodes. Calls entry 10 |
| eBPF inside sandbox guests | The guest is untrusted, so enforcement stays on the host side of the vsock |
| A WASM component-model ABI (a Legion WIT world) | Modern and typed, but no filters exist for it yet. Calls entry 6 records what would change the call |

---

## 7. Proto, struct, and config changes (sketch)

- **`HandlerConfig` becomes an enum** of `Authz`, `Proxy`, `Log`, `Dlp`, and
  `Plugin`, with the shared `timeout` and `onError` beside it. The policy
  hash format changes. This is breaking, and is stated in §10.
- **A new `NetPlugin` record** (name, kind, module source and digest,
  stages, usableBy, limits, configSchema), stored like `net_policy`, with
  the five RPCs in §6.E. Each has full proto comments and an entry in
  `tests/integration/acl_endpoints.rs`.
- **`NetAllocStatus` gains:**
  - `plugins` (health per plugin, as `handlers` reports today);
  - `kernel` (whether the allocation is attached, its map generation, and
    its pass, redirect, and deny counters).
- **The heartbeat carries plugin digests by content hash**, the same way
  policy bodies travel.
- **The node attribute `netKernel`.** `hostGuard` reads `active (ebpf)` or
  `active (nftables)`.
- **`configs/jobs/maximal.ncl`** gets the keyed handler forms and a plugin
  reference. `configs/policies/*.ncl` are rewritten in the new form in the
  same change.
- **New dependencies.** `aya` (A and C, Linux only, pure Rust at run time),
  a WASM runtime (B), and `proxy-wasm` (dev only, for the conformance
  modules).

## 8. Phases

There are two independent tracks. Every phase ships on its own and is
useful on its own.

**Track A: kernel**

| Phase | Ships | Value on its own |
| --- | --- | --- |
| **A0** | The feature probe, the `netKernel` attribute, and `net_bench` groups `kernel_pass_conn_rr`, `kernel_redirect_conn_rr`, `kernel_connect_cost`. A findings doc on coverage (`io_uring`, TFO, IPv6, connected and unconnected UDP) and restart survival | Measured numbers before any design is committed |
| **A1** | The host guard in eBPF (`connect4/6`, `sock_create`), with the same semantics and events as the `nftables` table, and pinned links | One mechanism, no `nft`, per-allocation counters. Proves the loader and the lifecycle on known behavior |
| **A2** | Kernel-enforced L4 egress for host-mode tasks: compiled maps, DNS capture with learned addresses, ring-buffer denials, the pass path, and kernel counters feeding the graph, idle detection, and metrics | Host-mode policy is real. Allowed flows cross no proxy |
| **A3** | Transparent capture: redirect into netd from host mode (for flows that need it) and from gated namespaces, plus the SNI/Host peek | Tools that ignore proxy variables work in gated tasks. Unlearned addresses are judged by name |
| **A4** | C: operator eBPF programs, the SDK, `plugin build` | Only on demand |
| **A5** | Kernel balancing for eligible plain upstreams | The hop disappears for plain L4 service calls |
| **A6** | `tc` enforcement for TAP-wired sandboxes | The same policy reaches TAP guests |

**Track B: plugins**

| Phase | Ships | Value on its own |
| --- | --- | --- |
| **B1** | E.1 (keyed handlers), E.2 (plugin objects, CLI, RPCs, MCP), E.3, E.4, and WASM at the `request` and `done` stages, policy objects only | Custom per-request logic with no second proxy |
| **B2** | The `body` and `response` stages, streaming bodies (SSE), and `usableBy = "jobs"` with shared data scoped per job | Scenario 1, and job-author plugins |
| **B3** | Credential signing host functions, and `dispatch_http_call` to declared targets | SigV4 and HMAC signing, and cross-node budgets through a service |
| **B4** | D: file-descriptor hand-off and node-local `authz` | Cheaper external handlers |

## 9. Testing

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
- **Benchmarks.** `net_bench` before and after each phase. A0 and B1 add
  their own rows, and the PR carries both numbers, as the datapath plan
  did.
- **Existing suites run unchanged** on macOS and on Linux nodes without
  `netKernel`.

## 10. Rollout and what breaks

Legion has no installed base to protect (AGENTS.md), so each of these is
a decision, listed for the PR descriptions:

- **Host-mode egress chains become enforced** on capable Linux nodes. A
  task that dialed past its own chain now gets `EPERM`. That is the right
  default: a policy you write should hold.
- **Handler syntax changes** to keyed variants. Every chain with handlers,
  `maximal.ncl`, and `configs/policies/*.ncl` change in the B1 PR. There is
  no alias for the old form.
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

## 12. Acceptance (per phase)

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
