# Judgement Calls: Mesh Interfaces Plan

Calls behind [`plans/mesh-interfaces-plan.md`](../plans/mesh-interfaces-plan.md).
Entries are ranked by project impact, highest first; an entry's number is its
rank. Each says what evidence would overturn it.

Entries 1–21 are the plan's own calls, made before any code. Entries 22
onward were made while executing it; they constrain less than the plan's
architecture, so they rank below it, and among themselves by impact. Track
B's (the WASM plugins) are interleaved at their rank.

This file began as `egress-plugins-proposal_calls.md` (merged in
[#49](https://github.com/mikenomitch/legion/pull/49)). That proposal's ten
entries are kept here, renumbered to their rank among the surface calls that
the merge added. Entries 2 and 16 were revised in the merge, because the plan
cut operator eBPF programs from its phases (entry 20). Each says so.

---

## 1. eBPF classifies flows; request-level plugins run in netd

**Call.** The ask was "egress proxy code that is maximally efficient and
pluggable, maybe eBPF plugins that are easy to define." The proposal reads
that as two problems and gives each its own tier: eBPF decides at
`connect()` whether a flow exists and where it goes (deny, pass, redirect),
and anything that reads a request runs inside netd as a WASM plugin.

```nickel
# kernel: nothing to write; this rule compiles to a map entry
allow = ["*.pypi.org"],
# netd: a plugin at the response stage
handlers = [{ plugin = "llm-token-meter", config = { budget = 2000000 } }],
```

**Alternatives.** (a) eBPF all the way down: `sk_msg`/`sk_skb` programs
that parse HTTP in the kernel, so "plugins" are eBPF everywhere. (b) WASM
all the way down: no kernel tier, every governed flow through netd, plugins
at every stage including connect.

**Trade-off.** Two mechanisms to document instead of one, and two
evaluators (kernel maps and `evaluate_connect`) that must agree — the
property test in §9 is the price. Revisit (a) if a use case needs payload
inspection on plaintext flows that cannot be pointed at an upstream.

**Why (performance and extensibility, both served).** eBPF cannot read
inside the task's TLS, hold a credential, make a call, or buffer a body,
and it sees nothing of a gated sandbox's vsock traffic — so it cannot be
the proxy-code plugin system, however easy the SDK. It is unbeatable at the
one thing userspace cannot do: letting a flow skip the proxy. Splitting by
stage gives each tool the job it is good at.

---

## 2. Nobody loads custom kernel code; rules compile to maps

*Revised in the merge.* The proposal kept operator eBPF programs as a last
phase. The plan cuts them to on-demand work (entry 20), so this call now
covers operators as well as job authors.

**Call.** The kernel tier runs one fixed, Legion-owned program set. A job
author's or a policy object's rules are compiled into map entries. No
custom eBPF program is loaded by anyone in this plan. If operator programs
come back on demand, they stay policy-objects-only and need unscoped
`net:write`, as the proposal had them.

```nickel
# a policy object; this compiles to map entries, not to a program
egress = { policy = "deny", rules = [{ when = { port = 25 }, action = "deny", reason = "no SMTP" }] },
```

**Alternatives.** (a) Let job authors ship eBPF programs for their own
tasks, verified by the kernel. (b) A small expression language
(`dst.port in [25, 465] && labels.tier == "free"`) that Legion compiles to
BPF bytecode, so authors get logic without writing eBPF. (c) The proposal's
middle ground: operator programs from policy objects.

**Trade-off.** Kernel-speed custom logic does not exist. A connect-time need
that rules cannot express goes through a WASM `connect`-stage plugin, which
forces a redirect and costs a hop.

**Why (security first, then simplicity).** The verifier is the only thing
between a tenant's program and the kernel, and verifier bugs have been
privilege escalations — the bpfman model (privileged daemon, RBAC-gated
program objects) is the industry answer for the same reason. (b) is safe
but is a second policy language with a compiler whose output must match
`evaluate_connect` exactly; rules plus selectors already express every
scenario in the plan. (c) is reasonable, but no scenario needs it yet
(entry 20).

---

## 3. Every hop is one typed `Handler` in one compiled pipeline

**Call.** A handler entry is a keyed variant (entry 13) backed by one
`Handler` trait with declared stages and placement, listed in one `HANDLERS`
table. The validator, every datapath path, `net policy show`, and the
manager's endpoint feed read that table, and every path runs the same
pipeline compiled once per (policy hash, upstream). A WASM plugin is one more
implementation of the trait.

```
egress.handlers[0] dlp inspects request bodies at stage "body", but
egress.protocol is "tcp". Set protocol = "http", or put it on an upstream
whose `to` is http:// or https://.
```

**Alternatives.** (a) Keep the per-path dispatch (`handlers::run` for
`authz`/`proxy`, `HttpSettings::new` for `dlp`/`log`) and fix the chain gap
in place. That is the smallest diff, and phase 0 does exactly this as a bug
fix. (b) Make WASM plugins the only extension point and leave the built-ins
hard-coded in their paths.

**Trade-off.** A refactor through every listener path and the per-request
path, gated by `net_bench` (S1 must be within noise). A trait with five
stage methods that most implementations leave at their defaults. One dynamic
call per hop, which is nanoseconds against a microsecond hop.

**Why (extensibility, and correctness by construction).** The phase-0 gap
exists because two lists of handlers exist and one path was never handed the
chain's. Any per-path dispatch reproduces that with the next kind, and B adds
the next kind. `BALANCERS` already showed that one table read by validation
removes the "accepted but never run" class. With (b), the built-ins and the
plugins would run through two different mechanisms, which keeps the problem.

---

## 4. Allowed flows that nothing reads pass without entering netd

**Call.** On a capable node, an allowed host-mode flow with no HTTP rule,
handler, or plugin on its path is passed by the connect hook and never
touches the worker. Accounting for it comes from kernel counters.

```console
$ legion net policy show batch-etl
egress  protocol tcp  ·  enforced in the kernel on 12/12 nodes
  ...
  allowed flows: connect directly (no proxy)
```

**Alternatives.** Keep every governed flow in netd (uniform per-connection
accounting, one code path) and use eBPF only for deny and for transparent
redirect.

**Trade-off.** Pass flows have no userspace view: no per-connection
duration in the edge ledger, bytes per allocation rather than per flow, and
the service graph, idle detection, and metrics read two sources (netd's
ledgers and kernel counters). Revisit if the graph or idle detection proves
inaccurate on pass flows.

**Why (performance).** The datapath plan already made the per-flow hop
cheap; the remaining cost (setup latency, a wakeup per round trip, worker
CPU, dropped flows on restart) is paid for a decision made once, at
connect. Passing is the only way to make that cost zero, and it is what
makes the restart story ("the datapath outlives the worker") true for most
flows without the `legion netd` process split.

---

## 5. Gated stays a namespace; the kernel tier adds transparency, not a new boundary

**Call.** A gated task's flows always go through netd. On a capable node the
connect hook rewrites them to the in-namespace capture listener, so tools
that ignore proxy variables work, but nothing is passed and the namespace
remains the boundary.

**Alternatives.** On capable nodes, run gated containers in the host
namespace with the cgroup hooks as the boundary, so their allowed flows
pass too — the efficient choice.

**Trade-off.** Gated containers (the Linux default for `container`) do not
get the zero-hop path; a throughput-bound trusted container has to choose
`mode = "host"` on an enforced node and accept a cgroup-hook boundary
(open question 6).

**Why (simplicity of the security story).** "No route except Legion's
listeners" holds even if a program fails to attach or a kernel feature is
missing; a cgroup hook holds only while it is attached, and a root task can
detach it. A boundary that degrades silently on some kernels is not one an
admin can reason about at 3am.

---

## 6. An upstream's scheme is its protocol; a policy never changes how a stream is parsed

**Call.** Only the upstream's `to` scheme turns on HTTP parsing. A chain's
`protocol = "http"` says which rules may be written. On a `tcp` upstream
those rules are evaluated with connect-time facts, and a verdict that needs
the request head is a denial that names the upstream.

```
net.denied  policy/prod-web#1  upstream 'db' is tcp://, and the first rule
that could match it needs a path. Hint: allow 'db' by host ahead of path
rules, or write to = "http://db" if it speaks HTTP.
```

**Alternatives.** (a) Today's rule: the effective policy's protocol forces
HTTP parsing on every upstream of the allocation. This is the maximal
enforcement reading, because a `require` HTTP policy then reads every
request. (b) Protocol detection, Linkerd's model. (c) Let an HTTP policy
force parsing on external upstreams only.

**Trade-off.** A `require` HTTP policy can no longer see inside a `tcp`
upstream. If a job author declares `tcp://api.github.com:443` and speaks TLS
from the task, path rules cannot apply, and the flow is denied rather than
inspected. That is the same outcome as today in practice (an HTTP parser
fails on TLS bytes), but an operator who wants inspection now has to get
authors to declare `https://` upstreams, and `doctor` and `job validate` are
what make that visible.

**Why (simplicity: you can read the cost off the config).** The handlers
plan made "protocol is never inferred" a rule so a reviewer can read a
policy's semantics and cost from its text. Today that is true for chains and
false for upstreams, and it breaks Postgres under a pool-wide web policy
(plan scenario 6). Consul's `service-defaults` and Kubernetes' `appProtocol`
both declare protocol on the thing being dialed. (b) is wrong for any
protocol where the server speaks first, which is why Linkerd needs "opaque
ports".

---

## 7. `service.allow` is the one caller list, compiled to an ingress rule

**Call.** `service.allow` takes job id globs (today's meaning) and workload
identity globs, compiles both to one matcher, and is enforced in the
endpoint feed always and at the receiving node when the service is on the
mesh. `tls.requireIdentity` is removed. `ingress.rules[].when.identity`
stays for identity combined with a path.

```nickel
service = { port = "http", mesh = true, allow = ["web", "*:billing:*:worker"] },
```

**Alternatives.** (a) Drop `service.allow` and have everyone write ingress
rules with `identity`. That is one mechanism, but the common case gets
verbose and loses the feed's fast close for a denied consumer. (b) Keep all
three and document them better.

**Trade-off.** `allow` has two enforcement points of different strength, and
which one applies depends on `mesh`. Without the mesh it is still not
cryptographic, as today, and `service doctor` has to say so. A bare entry
like `web` now means `*:web:*:*`, every group and task of job `web`, which is
what it meant in practice.

**Why (simplicity).** "Who may call this service" is the question an admin
asks at 3am, and today it has three answers that can disagree. Consul's
intentions are the precedent: one list, enforced at the destination. (a)
makes the simple case harder to get right in order to make the rare case
uniform.

---

## 8. The mesh is a switch on the service, off by default; TLS lives on ports; chains carry only policy

**Call.** `service.mesh = true` replaces
`ingress.tls = { mode = "mutual", certFrom = "legion", clientCa = "legion" }`.
Non-mesh TLS moves to `network.ports[].tls`, and an external upstream's trust
bundle to the upstream's `ca`. `tls` is removed from chains. `mesh` defaults
to `false`. That was an open question in the plan's first draft, and the
owner ratified the lean.

```nickel
# before
network = { ingress = { tls = { mode = "mutual", certFrom = "legion", clientCa = "legion" } } },
# after
service = { port = "http", mesh = true },
```

**Alternatives.** (a) Keep TLS on chains, with a `tls = "mesh"` shorthand.
That is the smallest change. (b) A task-level `network.mesh` switch that
covers every port. (c) Mesh on by default, since AGENTS.md asks for the
default that is right for a new cluster.

**Trade-off.** A policy object can no longer push certificate material onto
the tasks it matches, which `certFrom = { kv }` on a policy object does
today. A platform team that did that has to move certificates into the jobs.
A policy object can still impose `mesh = true`. Off by default means
service-to-service traffic is plaintext unless someone turns the mesh on, or
an operator imposes it with a policy object.

**Why (simplicity).** A chain is policy: what may pass. TLS is how the port
is served, and it belongs with the port, as Gateway API puts
`BackendTLSPolicy` on the backend rather than the route. With (a), TLS
modes would still mean different things on ingress and egress. (b) is too
coarse: a task can serve a public `https` port and a mesh service port at
once. (c) loses because a mesh port refuses every client from outside
Legion: default-on would break anything that reaches a service directly (a
load balancer, a developer's `curl`) the day it was deployed. Revisit for
services nothing outside Legion calls, once `net_bench` has the handshake
cost.

---

## 9. Hostnames reach the kernel through the resolver; DNS is captured

**Call.** All DNS from governed tasks is rewritten to the allocation's
resolver; allowed answers write their addresses (with a TTL floor) into the
allocation's learned-address map; a connection to an address the resolver
never returned is redirected and judged by SNI or `Host`, with the dialed
address checked against that name's own resolution.

**Alternatives.** (a) Peek SNI on every TLS flow under a hostname rule — no
DNS dependency, but every such flow enters netd, which undoes entry 4.
(b) Learned addresses only, deny anything unlearned — simplest, but breaks
clients with cached answers or `/etc/hosts` entries.

**Trade-off.** DNS capture changes behavior for host-mode tasks (their
`resolv.conf` is no longer what answers); DoH servers must be allowed by
name; CDN-shared addresses still permit fronting, as with any SNI- or
address-based control. A resolver address is needed for every enforced
allocation (open question 5).

**Why (performance, with correctness kept).** Cilium's `toFQDNs` shows the
resolver-taught kernel works at scale; Legion already runs the resolver and
already judges names in it, so the data is free. The peek covers the cases
the resolver misses without making it the common path.

---

## 10. One source grammar, and one material fetch behind a provider trait

**Call.** Every field that names where a value comes from takes the same
`Source` (`{ kv }`, `{ identity }`, `{ file }`), restricted per field.
`FetchDatapathMaterial` replaces `FetchNetPolicy`, `GetIngressCertificates`,
and `GetUpstreamCredentials`, and a manager-side `MaterialProvider` trait
resolves each kind.

```nickel
tls = { cert = { kv = "certs/app.example.com" }, clients = { ca = { kv = "certs/admin-clients" } } },
```

**Alternatives.** (a) Keep one RPC per material kind, and add a `{ vault }`
source to each field when someone needs it. Each step is small. (b) Carry
material on the heartbeat reply itself.

**Trade-off.** One RPC whose authorization branches by kind, so a mistake in
that handler is broader than a mistake in one of three. A generic ref also
documents itself less well in the proto than three named RPCs. The plan
mitigates this with per-kind proto comments and per-kind ACL tests.

**Why (extensibility).** With (a), every provider is a cross-cutting change
(an RPC, a wants field, an input variant, a caller class, an ACL row), and
the `legion netd` process split would inherit that growing message set. The
Secrets Store CSI driver is the precedent: one consumer grammar, providers
behind an interface. (b) is rejected because bodies are fetched once per
hash to keep heartbeats small, and secret values never ride the heartbeat.

---

## 11. Plugins use the proxy-wasm ABI, not a Legion ABI or the component model

**Call.** WASM plugins implement the HTTP-filter subset of proxy-wasm 0.2.1;
Legion facts are properties (`legion.labels.tier`).

**Alternatives.** (a) A Legion WIT world on the WASM component model —
typed, modern, multi-language through `wit-bindgen`, and free of Envoy-isms.
(b) A Legion-specific core-WASM ABI tuned to Legion's facts.

**Trade-off.** Proxy-wasm carries Envoy's shape (properties as byte paths,
`Action::Pause` after a local reply, a root/HTTP context split) and an ABI
whose evolution Legion does not control.

**Why (extensibility through an existing ecosystem).** "Easy to write" is
mostly "an SDK, examples, and existing filters already exist" — proxy-wasm
has them and (a)/(b) have none. Revisit if the component model gains a
widely adopted HTTP-filter world, or if proxy-wasm's shape blocks a Legion
feature (signing with credential handles, entry 12, is the first test).

---

## 12. Plugins never hold credentials; signing is a host function over a handle

**Call.** Credential injection runs after plugins, exactly as it runs after
`authz`. Request signing (SigV4, HMAC) comes as a host function that signs
with a credential the upstream declares; the module passes a name, never
receives the key.

**Alternatives.** Give a plugin read access to declared credential values
so it can sign however it likes.

**Trade-off.** Every signing scheme Legion supports is a host function
Legion must write and maintain; a scheme without one is not available to
plugins.

**Why (security).** The credential-injection guarantee is "the value is in
the worker's memory and on the TLS connection to the origin, nowhere else."
A module with the value can encode it into a path, a header, or shared
data; a handle keeps the guarantee intact with plugins in the path.

---

## 13. Handler entries become keyed variants, breaking the flat form

**Call.** `{ handler = "authz", url = ... }` becomes
`{ authz = { url = ... } }`, with `timeout` and `onError` on the entry. The
trait and registry behind the variants are entry 3.

```nickel
# before
{ handler = "proxy", service = "egress-filter", proxyProtocol = true }
# after
{ proxy = { service = "egress-filter", proxyProtocol = true } }
```

**Alternatives.** Keep the flat form and add `plugin` as one more `handler`
value with a `name` field.

**Trade-off.** Every chain with handlers, `maximal.ncl`, and the shipped
policy configs change in one PR, with no alias; policy hashes change.

**Why (simplicity of the surface).** The flat bag already has fields that
mean something for one kind only (`url`, `cache`, `service`, `node`,
`port`), and each new kind would add more. Keyed variants match
`from = { kv }` and `certFrom = { kv }`, get one Nickel contract each, and
make a misplaced field an evaluation error that names the variant. AGENTS.md
says to break for a cleaner surface while there is no installed base.

---

## 14. One `to` per upstream; a bare name is a catalog service

**Call.** `service` and the external `name` + `to` pair merge into one `to`.
A bare, portless name (or one ending in `.svc.legion`) is a catalog service.
Anything else is external. `name` defaults to the service name and is
required for an external destination.

```nickel
# before
{ service = "db" },
{ name = "openai", to = "https://api.openai.com" },
# after
{ to = "db" },
{ name = "openai", to = "https://api.openai.com" },
```

**Alternatives.** (a) Keep the two shapes. (b) A `svc://db` scheme. (c)
Require `db.svc.legion` for catalog targets. This was an open question in
the plan's first draft. The owner ratified the rule, with `.svc.legion`
accepted but never required.

**Trade-off.** The rule misreads a dotless intranet host (`http://wiki`),
which must be written with its port. It also needs a clear submit error when
a bare name is not a valid service name.

**Why (simplicity).** The jobspec reads like the URL the task dials, the
scheme carries the protocol (entry 6), and there is one validation path
instead of a matrix. Nomad's upstream is one shape. (b) invents a scheme no
client understands. (c) is unambiguous, but makes every spec longer to save
the rare dotless external host a port number.

---

## 15. One dialable address per upstream; on Linux it is the VIP

**Call.** `LEGION_UPSTREAM_<K>`, `{{ service "<k>" }}`, and the `_HOST_`/
`_PORT_` parts all name one address: the VIP and canonical port on Linux, the
loopback port elsewhere. `_VIP_` is removed. Only the VIP is bound on Linux
unless `port` pins a loopback port too.

**Alternatives.** (a) Keep both addresses and document which to use. (b) Make
the name (`db.svc.legion`) the variable.

**Trade-off.** `LEGION_UPSTREAM_<K>` changes value on Linux. A client config
with a literal `127.0.0.1:<port>` has to pin `port`. Until A2 captures DNS
for `exec` tasks, the name does not resolve there, so the variables carry
addresses.

**Why (simplicity, and a bug removed).** Today the env var and the template
disagree on Linux (plan §1, problem 9), and every upstream spends a port from
the node's dynamic range for a listener the VIP already provides. (b) would
not work for `exec` tasks today.

---

## 16. A plugin is referenced by name; what it is lives on the object

*Revised in the merge.* The proposal's main reason was swapping a WASM
plugin for an eBPF program of the same name. With operator eBPF programs cut
(entry 20), the object no longer carries a `kind`, and the reason below is
what remains.

**Call.** Chains say `{ plugin = "llm-token-meter" }`. The module digest,
the stages it runs at, its limits, and who may use it are properties of the
stored plugin object.

**Alternatives.** Reference the module directly from the chain
(`{ wasm = { source = "oci://…", stages = […] } }`), so a reviewer sees the
whole plugin in the chain itself.

**Trade-off.** Reading a chain alone does not tell you what a plugin does or
where it runs. `show` and `doctor` have to say so (§6.S1), and stage
mismatches are caught at submit rather than by eye.

**Why (extensibility and review).** The plugin object is the one place its
trust level (`usableBy`), limits, and config schema are reviewed. An operator
can roll a new digest without editing every job that uses it. If operator
eBPF programs come back, the same name can point at either.

---

## 17. Six knobs and kinds are removed rather than reshaped

*Revised after review.* The first draft also removed `retries.on`,
`retries.methods`, and the `least_conn` balancer. The owner kept retries as
they are. `least_conn` was kept after a closer read of the code showed it
has a job `least_latency` does not do (below).

**Call.** The plan removes the `log` handler, chain-level `onError`,
`proxy.node`, `http.outlier`, the `random` balancer, and the reserved
`bridge`/`cni/*` modes (plan §6.0). `round_robin` and `least_conn`'s
tie-break start each listener's cursor at a random offset.

```nickel
# before
http = { outlier = { consecutive5xx = 5, ejectFor = "30s" } },
balance = "random",
# after
http = {},                  # ejection is always on in HTTP mode, with those numbers
balance = "round_robin",    # now starts at a random offset per listener
```

**Alternatives.** (a) Keep every knob and reshape only the confusing ones.
(b) The first draft's longer list.

**Trade-off.** A user who tuned ejection, or who relied on `random`'s
per-pick independence, loses that. Each is one registry row or field to
bring back, and a concrete request for one is the evidence to do so (open
question 9).

**Why (simplicity).** Each removal duplicates something that stays (`log`
is `accessLog`, `node` is `service` + `local`, outlier ejection overlaps
`least_latency`'s failure penalty, and `random` exists only to avoid every
new `round_robin` listener starting at endpoint 0, which a random start
offset fixes in `round_robin` itself), doesn't work today (chain
`onError`), or reserves a name for a feature that doesn't exist.

The first draft argued that `least_latency` covers `least_conn` because it
weighs in-flight load. That is only half true. `least_latency` scores
`latency × (in-flight + 1)` over two random endpoints, so it deliberately
sends more connections to nearby, fast endpoints (the public doc says so).
`least_conn` scans every endpoint and keeps connection counts even
regardless of distance. For long-lived connections (database pools, gRPC
channels, websockets), where an endpoint's share of connections is its
share of load for hours, even counts are the point. `round_robin` can't do
it either, because it ignores which connections have closed. So
`least_conn` stays. Retries stay at the owner's call: `on` and `methods`
are how a user opts into retrying `reset` or a non-idempotent method, which
is rare but not wrong.

---

## 18. Transparent capture is a connect-time rewrite plus a map

**Call.** The connect hook rewrites the destination to netd's capture
listener and records the original destination, keyed by the socket's
network namespace and bound source address and port (filled by `sockops`);
netd reads it per new connection. This is Merbridge's mechanism, attached
per allocation cgroup rather than node-wide.

**Alternatives.** (a) `sk_lookup` with a local default route in the
namespace: the accepted socket's local address *is* the original
destination, no map needed. (b) `nftables` `redirect` matched by cgroup,
with `SO_ORIGINAL_DST` from conntrack — no new dependency. (c) A TUN and a
userspace TCP stack (the handlers plan's deferred phase 7).

**Trade-off.** One map lookup per redirected connection, and a key that
must include the namespace (every gated namespace has `127.0.0.1`).

**Why (one mechanism for both modes).** The connect hook runs before
routing, so it works in a gated namespace with no route and in the host
namespace alike; (a) needs per-namespace routing tricks and does nothing
for host mode, (b) needs a dummy route in gated namespaces and pays
conntrack on every flow, and (c) moves every packet through userspace. (b)
remains the natural fallback if A0 finds the map approach fragile.

---

## 19. The chain-handler and chain-`onError` bugs ship as a fix before S1

**Call.** Phase 0 makes chain-level `dlp`/`log` run and makes hops inherit
the chain's `onError`, as standalone fixes, before S1 replaces the code
around them.

**Alternatives.** Fold both into S1, which removes the second dispatcher and
chain-level `onError` anyway.

**Trade-off.** Some of the fix's code is replaced by S1, and the
chain-`onError` inheritance is written and then removed.

**Why (security).** A `require` policy that says it blocks bodies holding API
keys, and silently does not, is a security bug in a shipped feature. S1 is a
multi-file refactor gated on benchmarks, and the fix is small. A policy
should do what it says from the next release, not from the end of a track.

---

## 20. Operator eBPF, fd hand-off, kernel balancing, and TAP `tc` are cut to on-demand

**Call.** Four items from the #49 proposal leave the plan's phases for
on-demand TODO items (plan §6.F): its C (operator eBPF programs, an SDK, and
`net plugin build`), its D (`handoff = true` and sidecar `authz`), its A5
(kernel balancing), and its A6 (`tc` for TAP-wired sandboxes).

**Alternatives.** Keep them as late phases, as the proposal did ("C last,
only on demand"; "D when a `proxy` user asks").

**Trade-off.** Anyone who wants kernel-speed custom logic, zero-copy proxy
hand-off, or zero-hop service calls waits for a concrete request and a
follow-up plan. The proposal's design work for them stays in git history
(PR #49), not in this document.

**Why (simplicity).** None of the plan's scenarios needs them. Each adds a
user-facing knob or a second mechanism, and C would add a second plugin kind,
a pinned build toolchain, and the riskiest privilege in the system. A late
phase in a plan still reads as a commitment and pulls design weight onto
everything before it. A TODO item does not.

---

## 21. One plan, renamed, rather than a second plan that supersedes the first

**Call.** The mesh-surface review was merged into the #49 proposal, and both
files were renamed (`egress-plugins-proposal*.md` →
`mesh-interfaces-plan*.md`). This is one calls file.

**Alternatives.** (a) Keep the proposal and head it `SUPERSEDED`, pointing
at a new plan. That is the repository's convention when one plan replaces
another. (b) Five small surface plans beside the proposal.

**Trade-off.** One long plan. Links to the old path from outside the
repository break. In-tree links are fixed in the same change.

**Why (simplicity of the record).** Nothing in the proposal was built or
overturned: its content lives on here, revised where the merge changed it
and saying so. A superseded stub would be a second document to keep honest.
The tracks also depend on each other (B needs S1; A reads S1's stages and
S2's protocol rule; B3 signs with S5's refs), so separate plans would each
restate the others. The owner asked for a single plan.

---

## 22. The plugin engine is wasmi, an interpreter, not wasmtime

**Call.** Plugins run in [wasmi](https://github.com/wasmi-labs/wasmi) 2.0
(open question 3). Every callback gets fuel, and every instance a memory
cap.

**Alternatives.** wasmtime 49 with Cranelift: it compiles modules ahead of
time and interrupts by epoch.

**Trade-off.** An interpreter runs compute-heavy plugins many times slower
than compiled code. A header-stage plugin costs tens of microseconds, not
single digits. There is no clock-driven interruption either (entry 25).

**Why (simplicity, performance where it counts).**
- **Binary size.** Measured with stripped LTO builds of an otherwise empty
  binary (scratch probes, 2026-10-18): wasmi adds 1.8 MB to the worker,
  wasmtime with Cranelift about 10 MB. The worker ships at ~26 MB, and a
  40% size jump for a feature most nodes never use is the wrong default.
- **Build cost.** wasmtime also roughly doubles a clean build.
- **Cost profile.** Plugins are meant for header logic, metering, and
  signing, where a callback is a few thousand instructions. The no-op
  `http_rr` row is the number to watch.

Overturn it if a real plugin's callback time dominates its request, or if
the shipping binary stops caring about 8 MB. The host code is
engine-agnostic above `plugins::vm`.

---

## 23. `usableBy = "jobs"` widens, never narrows; the scope is the reference, not the plugin

**Call.** `usableBy = "jobs"` means jobs *and* policy objects may reference
the plugin; `policies` means only policy objects. A reference runs under a
scope:
- **A policy object's hop** (source `policy/…`) runs under the plugin's own
  scope (`policy`).
- **A job's own hop** (its chains or an upstream's handlers) runs under
  `job:<id>`.

Instances, shared data, and module metrics are per scope.

```nickel
{ plugin = { name = "llm-token-meter", config = { budget = 100000 } } }   # in a job: scope job:agents
```

**Alternatives.**
- `jobs` meaning jobs only.
- Scopes per allocation.
- Scopes per (plugin, job) for policy references too.

**Trade-off.** A policy that applies one plugin to many jobs shares one
counter across them. A per-job budget from a policy object needs the module
to key its shared data by `legion.job`.

**Why (ease, safety).**
- **The plan only reads one way.** It says a job's plugin never sees
  another job's counters (B2's acceptance test). A policy object is written
  by an operator who already sees every job it selects.
- **Per-allocation scopes would break metering.** A replicated job's
  instances could not share a budget on a node.
- **Policies keep their access.** Making `jobs` exclusive would force an
  operator to apply a plugin twice just to use it from a policy.

---

## 24. A plugin's object reaches the node as a material ref, not inlined into policy bodies

**Call.** Nodes fetch a referenced plugin by name through
`FetchDatapathMaterial` (kind `plugin`), versioned by its cluster-wide
revision. The manager serves it only to a node whose allocations reference
it, and only if the node's pool is in `nodePools`. Signing keys travel the
same way (kind `plugin_credential`). Policy bodies keep `{ plugin = { name,
config } }`. The hop binds late: it looks the plugin up when a request
arrives.

**Alternatives.** Resolve every plugin hop into the merged policy body at
heartbeat time, so a plugin update changes the policy hash and reaches nodes
as a new body.

**Trade-off.** A hop can run before its plugin has arrived. It then fails
with "not on this node yet", by its `onError`. The first requests after a
new reference can fail closed for one heartbeat plus the module fetch.

**Why (simplicity, extensibility).**
- **One revision per plugin.** The plan says the digest reaches the node as
  an S5 ref, and this is that. A plugin update is one ref with a new
  revision, not a recomputed hash for every policy and allocation that uses
  it.
- **Upstream handlers are covered.** A job's upstream handlers, which live
  on the allocation payload and not in a policy body, get the plugin the
  same way.
- **Module fetching stays independent.** It is decoupled from the policy
  merge.

---

## 25. Deadlines are fuel plus a clock check after the callback

**Call.** `limits.deadline` is enforced in two ways:
- **Fuel.** Each callback gets fuel in proportion to its deadline
  (5 million units per ms, from a measured 2.4 million units per ms on a
  2.1 GHz Xeon). A runaway stops within about twice its deadline there.
- **The clock.** A callback that returns past its deadline counts as
  failed.

Starting an instance gets ten callbacks' worth of fuel and no deadline.

**Alternatives.**
- Run callbacks on a separate thread and abandon them at the deadline.
- wasmtime's epoch interruption (entry 22).

**Trade-off.**
- A spinning module holds a datapath thread for up to about 2× its
  deadline, longer on slow cores.
- A module that is slow but finishes is reported late, not stopped.
- Fuel is per instruction, not per microsecond, so the same plugin gets
  more wall time on a slow node.

**Why (simplicity).**
- **wasmi has no interruption.** A thread per callback costs a hop per
  request and leaks a stuck thread per runaway.
- **Fuel bounds every case.** The bound is deterministic, and the clock
  check keeps the documented meaning honest for callbacks that return.
- **Start-up stays off the deadline.** Instance start runs once per pool
  slot, so holding it to a 10 ms deadline would fail cold starts in debug
  builds and on slow nodes for no safety gain.

---

## 26. Plugin objects carry `nodePools`, and policies that use one must stay within it

**Call.** The plan scopes `net:write` on a `usableBy = "policies"` plugin
"by pools" without saying whose pools. The object gets a `nodePools` glob
list; empty means every pool.
- **ACL.** Applying a pool-scoped plugin needs `net:write` on each pool.
- **Policies.** A policy that references it must select node pools within
  those globs.
- **Nodes.** The manager serves the plugin only to nodes in those pools.

```
policy 'p' egress.handlers[0]: plugin 'meter' runs only on node pools gpu-*; the
policy must select node pools within those (selector.nodePools)
```

**Alternatives.**
- Scope by the referencing policies' pools.
- No pool scoping at all, with every plugin cluster-wide.

**Trade-off.**
- One more field.
- A pool admin's plugin cannot be used by a cluster-wide policy.
- Glob containment is checked by matching the policy's pool strings
  against the plugin's globs, so a policy glob only passes when a plugin
  glob matches its text.

**Why (security, ease).** A pool admin must not be able to ship code to
every node in the cluster. Without the field, the scope would have nothing
to bind to.

---

## 27. Loopback is the guard's; the kernel chain judges what leaves the node

**Call.** On a node with the classifier, a host-mode task's connection to a
loopback address (`127.0.0.0/8`, `::1`) is never judged by its egress chain.
The host guard still applies to it (Legion's control ports, other
allocations' VIPs and upstream ports), and so does DNS capture on port 53.

**Alternatives.** Judge loopback like any destination, so a `deny` chain
refuses `127.0.0.1:5432` unless a rule allows it.

**Trade-off.** A chain cannot stop a task from reaching another service on
the node's loopback that the guard does not cover (an operator's own
daemon). That is the same reach host mode has without the classifier.

**Why (ease).** Host mode means the task shares the node's network on
purpose, and every real chain would have to start with
`allow = ["127.0.0.0/8"]` to keep a task talking to its own sidecars and
to itself. A gated task's loopback is its own namespace, and the gated proxy
does not judge it either, so this keeps the two modes saying the same thing:
egress policy is about what leaves.

---

## 28. Only the mesh CA vouches for nodes; a port's own client CA vouches for names

**Call.** On a port with its own `tls.clients.ca`, a verified client
certificate carrying a `legion://node/<id>` SAN is treated as the plain name
`legion://node/<id>`. Only on the mesh port does a node peer's PROXY v2
header name the workload it forwards for.

**Alternatives.** Keep what the BYO `clientCa` path did before: any verified
peer whose SAN reads `legion://node/…` is a node, whichever CA verified it.

**Trade-off.** A client of a mutual port cannot use a node certificate to
forward an attributed workload identity through that port. Nobody does this
on purpose; the mesh port is where attribution lives.

**Why (security).** A port's client CA is the operator's (or a customer's)
to sign with. Under the old rule anyone who could get a certificate with a
`legion://node/x` SAN from that CA could send a PROXY header naming any
workload and pass every `identity` rule. The fix is three lines in
`ingress::handle` and closes the hole for the new `clients.ca = { kv }` path
before it ships.

---

## 29. A captured flow's name must own the address it dialed, and the original address is what is dialed

**Call.** The capture listener reads the SNI or `Host` of a redirected flow
and allows it only if the address the task dialed is one of that name's
addresses (`Legion-Denied-By: capture/name` otherwise). It then dials the
original address, not a fresh resolution of the name.

**Alternatives.** (a) Trust the name and dial whatever it resolves to now,
as the gated proxy does for a `CONNECT host:port`. (b) Judge by address
only and ignore the name.

**Trade-off.** A flow to a CDN address that rotated out of the name's
current answer between the task's lookup and its connect is refused, and
the task retries. Plaintext HTTP under an HTTP chain is served request by
request, so a later request on the same connection may name another host
and is dialed by that name.

**Why (security).** Without the check, a task that wants a denied host can
dial its address and put an allowed name in the SNI. With (a), the same
trick is harmless but the flow silently goes somewhere other than where the
task sent it, which breaks clients that pin an address. The check costs one
resolution per captured connection.

---

## 30. The programs are C built with clang, checked in as an object

**Call.** `bpf/legion_net.bpf.c` is plain C with its own helper
declarations (no libbpf headers), built by `bpf/build.sh` with clang 18
and checked in as `bpf/legion_net.bpf.o`. The worker loads it with `aya`.
CI rebuilds it and fails when the object is stale.

**Alternatives.** The plan's lean (open question 2): `aya-ebpf` sources in
Rust, objects built in CI and committed.

**Trade-off.** Two languages in the datapath, and an ABI header
(`bpf/legion_net.h`) mirrored by hand in `kernel/abi.rs` (a size test pins
each struct). A clang or kernel-header update can change the object's bytes
and trip the CI check without any behavior change.

**Why (simplicity).** `aya-ebpf` needs a nightly toolchain and
`bpf-linker`; C needs clang, which every Linux CI image has. The programs
are small, use only stable UAPI context fields (so no CO-RE is needed), and
the verifier accepted them as written. `cargo build` still needs only
`protoc`.

---

## 31. Plugin modules are single files: no `oci://` yet

**Call.** `module.source` accepts `https://`, `http://`, `s3://`, and
`file://`, with a required `sha256:` checksum. Each source is fetched with
`ArtifactStager::fetch_into_store` into the node content store.

**Alternatives.** Accept `oci://…@sha256:` as the plan's example did.
ORAS-published WASM artifacts would then be pulled as layers.

**Trade-off.** Teams that publish modules to an OCI registry must also host
the `.wasm` over HTTPS or S3.

**Why (simplicity).** The content store's single-flight, cached,
digest-verified path handles single files only. An OCI fetch returns a
manifest digest, not the module's, so caching it needs a second code path
and a manifest-to-blob mapping. Recorded in TODO.md.

---

## 32. A failed plugin answers `503` with `Legion-Denied-By` naming the hop, not `plugin/<name>`

**Call.** A trap, exhausted fuel, a passed deadline, or a module that will
not start answers:

```http
HTTP/1.1 503 Service Unavailable
legion-denied-by: policy/meter#0
legion-denied-reason: plugin llm-token-meter exceeded its 10ms deadline (12931 µs)
```

A plugin's own refusal (`send_http_response`) keeps its status and its own
`legion-denied-reason`, and gets `legion-denied-by` with the hop's source
when it did not set one.

**Alternatives.** `Legion-Denied-By: plugin/<name>`, as the plan sketched.

**Trade-off.** The header names the policy hop, not the plugin, so a reader
looks at the reason for the plugin's name.

**Why (ease).** Every other denial (rules, `dlp`, `authz`, the host guard)
names its source in that header, and the denial ledger, the `net.denied`
event, and the access log key on it. One plugin used by two policies is two
different decisions, and the hop source tells them apart.

---

## 33. Request bodies are buffered; response bodies stream chunk by chunk

**Call.** The `body` stage sees the whole request body in one callback,
with `end_of_stream = true`, up to `limits.maxBodyBytes`. The response body
reaches the plugin one chunk at a time, as the upstream sends it, through a
channel. A `Pause` holds the bytes until the next chunk, up to the same cap.

**Alternatives.** Stream request bodies too.

**Trade-off.**
- A plugin cannot act on the first bytes of a large upload before the rest
  arrives.
- Request bodies over the cap are refused (or passed, under
  `onError = "allow"`).
- The response is sent chunked without `content-length` whenever a plugin
  reads its body.

**Why (simplicity).**
- **Request bodies were already buffered.** Every HTTP path buffers them
  for `dlp` and for retries (a retry re-sends the body). Streaming them
  would mean a second, retry-less request path.
- **Response streaming is the case that matters.** Server-sent events from
  an LLM API are what scenario 1 meters, and buffering them would hold a
  stream until it ends.

---

## 34. Stored rows are not migrated across the reshape

**Call.** `AllocUpstream.service` became `key` (S2), `AllocNetwork.ingress_tls`
was replaced by `AllocPort.mesh`/`tls` (S3), and chains lost `tls` with no
store migration. Rows written by an older build are not rewritten.

**Alternatives.** The plan's §7 sketch: a migration in
`storage/migrations.rs` that renames `service` to `key` in every stored
allocation network.

**Trade-off.** An allocation row written before this change does not parse
under the new build, so a cluster upgraded in place loses its running
allocations' datapath state and they are rescheduled. Stored policy objects
that carried `tls` fail to load and must be re-applied without it.

**Why (simplicity).** AGENTS.md: Legion has no installed base to protect, and
compatibility code for the old form is not added. A migration is code that
runs once on clusters that do not exist. The PR description says what broke.

---

## 35. The programs attach once, to the cgroup parent

**Call.** One set of links, on the worker's cgroup parent, serves every
allocation: each program reads the caller's leaf with
`bpf_get_current_ancestor_cgroup_id` and looks it up in `ln_allocs`. A
socket under the parent that no known leaf owns gets the guard floor (key
0), as the `nftables` table's `unattributed` chain does.

**Alternatives.** The plan's sketch: attach to each allocation's leaf and
pin links per allocation.

**Trade-off.** Every process under the parent runs the programs, governed
or not (an unknown leaf costs one hash lookup and passes). Per-allocation
attach state is not visible in `bpftool cgroup tree`.

**Why (performance, simplicity).** Ten programs per allocation would be
ten link creations and pins on every placement and as many detaches on
every exit, on the heartbeat path; one set is created at worker start.
Nested cgroups under a leaf (a container runtime's) are covered by the
ancestor lookup either way.

---

## 36. Pins stay when the worker stops

**Call.** The worker never removes the classifier's pins on its own: a
worker that stops (or crashes) leaves the programs attached and the maps
pinned, and the next worker re-adopts them. `KernelNet::shutdown` removes
them, and nothing calls it outside tests.

**Alternatives.** The plan's text: a clean shutdown removes the pins, as
it says the `nftables` table is deleted.

**Trade-off.** A node that is decommissioned keeps a few idle programs on
its Legion cgroup parent until reboot. They govern only leaves under that
parent, which are gone with the worker's tasks.

**Why (security).** Tasks can outlive the worker (a restart, an upgrade,
a stop that leaves them running). Removing the pins on a clean stop would
turn every worker restart into a window where host-mode tasks are
unguarded and their chains unenforced. The `nftables` guard behaves the
same today: nothing deletes its table at shutdown.

---

## 37. Instances are pooled per (revision, scope, config); a request keeps its instance

**Call.** Each (plugin revision, scope, config) gets a pool of one slot per
core, up to 16, with instances started on demand. A request takes the first
instance no callback holds right now, else the next slot round-robin. It
keeps that instance for its stream context, locking it only while a callback
runs. Under light load every request lands on one cache-warm instance. A
broken instance is replaced for the next request.

**Alternatives.**
- One instance per plugin, locked per callback.
- A fresh instance per request.

**Trade-off.**
- Callbacks on the same instance serialize, so two requests that landed on
  one slot wait on each other's callbacks (never on each other's I/O).
- Shared data is host-side, so instances agree. In-module globals do not.

**Why (performance).**
- **Contexts live in instance memory.** A proxy-wasm stream context lives
  in the instance that created it, so a request cannot move.
- **The alternatives cost more.** A single instance serializes the node's
  plugin work. A fresh instance per request costs an instantiation plus
  `on_vm_start` and `on_configure` each time.

---

## 38. `legion_sign` is a foreign function with two algorithms

**Call.** Signing is `proxy_call_foreign_function("legion_sign", <json>)`,
with:
- `hmac-sha256` (`data` or `dataBase64`), returning the MAC.
- `aws4-hmac-sha256` (`date`, `region`, `service`, `stringToSign`),
  returning the hex SigV4 signature, with the signing key derived host-side.

Keys are declared on the object under `credentials`, as `{ kv = … }` only.
Applying the plugin needs `kv:read` on each path.

**Alternatives.**
- A Legion-specific host import (`legion_sign(...)`).
- Return the derived SigV4 signing key and let the module HMAC with it.

**Trade-off.** A module can use the key as a signing oracle for anything.
Two algorithms cover HMAC APIs and AWS, and others need a host change.

**Why (extensibility, security).**
- **Foreign functions are the ABI's extension point.** Any SDK can call
  them without a Legion crate.
- **The derived key would be a credential.** A SigV4 signing key is valid
  for a whole day, region, and service, so handing it over would give the
  module exactly what entry 12 forbids.

---

## 39. `dispatch_http_call` reaches only declared targets, judged like the task's own flows

**Call.** A call names one of the object's `targets`. netd sends it to the
declared scheme and authority with the module's `:path`, after:
1. the calling allocation's egress chain at connect time
2. the host guard, dialing the address the guard checked

Each call uses a short-lived client pinned to that address.

**Alternatives.**
- Let the module name any URL.
- Skip the egress chain, because the plugin object is operator-written.

**Trade-off.**
- A target change means re-applying the plugin.
- A pinned client per call costs a fresh connection, with no pooling.

**Why (security).**
- **A `usableBy = "jobs"` plugin runs on a job's behalf.** If calls skipped
  the chain, a plugin would become a way around the job's own egress
  policy.
- **Pinning closes a rebinding window.** Resolving twice would let a
  rebinding name pass the guard and then dial elsewhere.
- **Calls are rare.** Pooling can come when a measured plugin needs it.

---

## 40. The caller's group travels on the allocation payload

**Call.** `TaskAllocation` gains `task_group` and `task_service`. The worker
names a mesh caller `<region>:<job>:<group>:<task>` from `task_group`, and the
manager matches `service.allow` in the endpoint feed against the same string
and compiles the receiving side's `callers` from `task_service`.

**Alternatives.** (a) Keep reading `group` out of `task_config`, which is the
plugin config and almost never has one, so every caller was
`…:default:…`. (b) Look the group up from the task config on each feed
build.

**Trade-off.** Two more fields on a per-heartbeat message (both small, and
empty for tasks without a service).

**Why (correctness, performance).** S4's `*:billing:*:worker` form is
meaningless if every caller's group reads `default`; the bug predates this
plan and made `identity` rules on group silently never match. The payload is
already where the worker reads job and task from, and the feed is a
per-heartbeat path that should not grow store reads.

---

## 41. A host-mode task with an egress chain gets a VIP block

**Call.** On a Linux node, placement gives a host-mode `exec` or
`container` allocation whose egress chain is inline or comes from a
matching policy a VIP block, upstreams or not. Its `.1` is the
allocation's resolver, which teaches the kernel the addresses of allowed
names.

**Alternatives.** The plan's open question 5 named a node-wide resolver
that attributes queries through a connect-time map.

**Trade-off.** One more `/24` of the `127.42.0.0/16` space and one resolver
listener per such allocation.

**Why (simplicity).** The per-allocation resolver already exists and
already judges names with the allocation's chain; a node-wide one would be
a second resolver with its own attribution path. The address space holds
256 × 256 blocks, more than a node runs allocations.

---

## 42. One fetch per heartbeat, and policy bodies are a provider

**Call.** `FetchDatapathMaterial` is one call per heartbeat carrying every
ref the node wants. The manager resolves a ref in two steps: the
declaration (which `Source` the allocation declares for it) and a
`MaterialProvider` per source kind (`kv`, `identity`), plus a `policy`
provider for content-addressed bodies, which are not a `Source`.

**Alternatives.** (a) Keep three calls and add three more for port
certificates, client CAs, and upstream CAs. (b) Make `policy` a `Source`
variant (`{ policy = "<hash>" }`), so there is only one dispatch.

**Trade-off.** One RPC whose reply mixes kinds, so a per-ref error lives in
the entry rather than the status. Policy bodies get a provider that no
jobspec can name.

**Why (extensibility, simplicity).** A new secret store is one provider and
one `Source` variant; a new kind of material is one ref kind. Neither needs
an RPC, an ACL table row, or a netd change. A policy hash is not something a
user writes, so it stays out of the grammar users see.

---

## 43. Applying a plugin re-checks the policies that use it, not the jobs

**Call.** UpsertNetPlugin re-runs every referencing stored policy's checks:
stages against its chains, its config against the new schema, and its pools.
It refuses the write if one fails. Jobs already submitted are not
re-checked. Deleting a plugin that a policy uses is refused; deleting one
that only jobs use is allowed, and their hops fail with "not on this node".

**Alternatives.** Scan every job config on each plugin write.

**Trade-off.** A plugin update can leave a running job with a config the new
schema rejects. The module gets that config anyway, and the job's next
submit is refused.

**Why (simplicity, performance).** Policy objects are few and already
listed on every write. Job configs are many, and a plugin write would
become a full scan. Recorded as a known limitation in the public doc.

---

## 44. A plugin cannot change where a request goes

**Call.** A module may rewrite `:path` and any header. Changes to
`:method`, `:authority`, and `:scheme` are ignored, and the upstream path
sets `host` from the upstream itself.

**Alternatives.** Apply every pseudo-header change, as Envoy does.

**Trade-off.** No method rewrites and no authority-based routing from a
plugin.

**Why (security).** The destination and method were judged by the chain's
rules before any hop ran. Honoring a later change would let a plugin, and so
a job's own `usableBy = "jobs"` plugin, send a request somewhere its policy
never judged.

---

## 45. A chain beyond 64 rules or 64 hosts is judged by netd only

**Call.** The kernel holds up to 64 connect-time rules and 64 distinct host
globs per chain. A chain beyond either is not compiled; the allocation's
`NetKernelStatus.reason` says so, and its egress behaves as it does on a
node without the classifier.

**Alternatives.** Larger fixed arrays, map-in-map rule sets, or an LPM trie
for CIDR rules.

**Trade-off.** A very large chain loses kernel enforcement in host mode
(gated tasks are unaffected).

**Why (performance, simplicity).** A bounded linear walk keeps first-match
semantics exactly (an LPM trie does not), passes the verifier on old
kernels, and measured within noise of no classifier at 32 rules. Real
chains are a handful of rules; a policy that needs more is better written
with CIDRs.

---

## 46. A policy cannot make a port with its own `tls` the mesh port

**Call.** When a `require` or `default` policy turns `mesh` on and the
service's port carries its own `tls`, placement leaves that port on its own
TLS. A task that sets both itself is rejected at submit.

**Alternatives.** The policy wins and the port's `tls` is ignored, or
placement fails.

**Trade-off.** A policy that means "everything on the mesh" has a hole where
a task terminates TLS with its own certificate. `legion net policy show`
reports `mesh: on` for it while its catalog endpoint is not mesh.

**Why (simplicity).** The task's explicit certificate is the more specific
statement, and failing placement for a policy that a task cannot satisfy
would strand it. Revisit if an operator needs mesh enforcement to be
absolute; then the right shape is a submit-time refusal under `require`.

---

## 47. Jobspec shapes are validated by serde and Rust, not Nickel contracts

**Call.** S1's "one contract per variant" is implemented as typed serde
structs with `deny_unknown_fields`, plus a hand-written `Deserialize` for a
hop that names the stray field and the variant (`unknown field "url" in
proxy`). The parity test checks `HANDLERS` against `maximal.ncl` and the
docs table instead of against contracts.

**Alternatives.** Ship a Nickel contract library that jobspecs import.

**Trade-off.** Errors come at submit (or `job validate`), not at Nickel
evaluation, so an editor running `nickel` alone does not see them.

**Why (simplicity).** The repository has no contract library: every jobspec
field is validated in Rust at submit, and `job validate` runs the same code.
A second validator in Nickel would have to agree with the Rust one forever.

---

## 48. Chains refuse unknown fields

**Call.** `ChainConfig` is `deny_unknown_fields`, so a chain that still
carries a removed field (`onError`, `tls`) fails to parse:

```
unknown field `tls`, expected one of `protocol`, `policy`, `accessLog`, `allow`, `rules`, `handlers`
```

**Alternatives.** Accept and ignore unknown fields, as chains did before.

**Trade-off.** A typo in an optional chain field is now an error too.

**Why (simplicity).** A removed security field that is silently ignored is
the worst outcome: the policy reads as if it terminates TLS and does not.

---

## 49. Removing `log` drops per-upstream access logging

**Call.** With the `log` handler gone (§6.0), access logging is the chain's
`accessLog`, which covers every HTTP request of the allocation's chain.

**Alternatives.** Keep `log` as an upstream-only hop.

**Trade-off.** An allocation cannot log one upstream's requests and not
another's.

**Why (simplicity).** One way to turn on access logs. Nobody asked for
per-upstream granularity, and the access log already carries the upstream
on each line.

---

## 50. `net policy test` answers for `service/allow` only with `--identity`

**Call.** `TestNetPolicy` runs a mesh service's `callers` check only when the
request names an identity. Without one it answers as a non-mesh port would.

**Alternatives.** Treat a missing identity as the mesh port's unverified
caller, and deny it.

**Trade-off.** `legion net policy test api --from 10.0.0.9` reports the
rules' verdict even though the mesh port would refuse a caller that names no
workload.

**Why (ease).** The common question without `--identity` is about rules on
an address; denying every such test on a mesh service would hide them. The
proto comment says when the check applies.

---

## 51. Kernel denials are read once per heartbeat

**Call.** The worker drains the ring buffer and the counters on each
heartbeat, in the same blocking step that updates the maps, rather than
on a dedicated reader task woken by the ring buffer.

**Alternatives.** An `AsyncFd` reader that folds each event as it arrives.

**Trade-off.** A burst of more than the ring's 256 KiB (about 5,000 events)
within one heartbeat drops events; the counters still count every refusal,
so the metrics stay exact while some `net.denied` rows are lost.

**Why (simplicity).** Denials are reported on the heartbeat anyway, so a
reader task would buffer for the same deadline. One code path, no extra
task.

---

## 52. Shared data and module metrics carry across revisions on a node

**Call.** A new plugin revision replaces its instances. The per-scope shared
data and module metrics move to the new entry.

**Alternatives.** Reset them with the instances.

**Trade-off.** A revision that changes its shared-data layout reads the old
layout's values and must cope.

**Why (ease).** Scenario 1 is a token meter. Resetting every node's counts
on each plugin update would let an update reset a budget.

---

## 53. Module log lines go to the worker log, rate-limited

**Call.** `proxy_log`, and standard output or error, go to the worker's
tracing log under target `legion::net_plugin`. Each line carries the
plugin, the scope, and the context id, at the module's own level. Past 100
lines per second per plugin, the rest are dropped and counted.

**Alternatives.** Write to each allocation's log directory, next to
`net-access.log`.

**Trade-off.**
- A job author reading their own allocation's logs does not see their
  plugin's lines.
- A noisy plugin's lines are dropped.

**Why (simplicity).** Instances are pooled per scope, not per allocation
(entry 37), so a line has no single allocation to go to. The worker log
already carries the datapath's own lines. The cap stops a per-request log
call from flooding a node.

---

## 54. Ticks are accepted and never fire; shared queues are unimplemented

**Call.** `set_tick_period` returns `Ok` and never ticks. Shared queues,
gRPC, and foreign functions other than `legion_sign` return
`Unimplemented`.

**Alternatives.**
- Return `Unimplemented` for ticks too.
- Run a timer per instance.

**Trade-off.** A module that relies on ticks silently never gets one.

**Why (ease).**
- **The Rust SDK unwraps the tick call.** `Unimplemented` would make any
  module that sets a tick in `on_vm_start` fail to start.
- **Timers would be a scheduler.** Running a timer inside the datapath per
  instance is a background scheduler nobody has asked for.

The public doc says ticks never fire.

---

## 55. Test modules are checked in as built `.wasm`, with their sources

**Call.** `tests/netplugins/` holds:
- the sources of the conformance module and the documented example
- `build.sh`
- the two built modules, which the tests and `net_bench` load

**Alternatives.**
- Build them in `build.rs` or in CI, with `wasm32-wasip1` as a hard
  requirement for `cargo test`.
- Hand-written WAT.

**Trade-off.**
- About 225 KB of binaries in the tree.
- A source change without rerunning `build.sh` tests the old module.
- No CI step checks that the two agree.

**Why (ease).** Every contributor runs `cargo test` without a WASM target.
SDK-built modules exercise the real proxy-wasm Rust SDK, which WAT would
not. `configs/netplugins/read-only-free-tier.ncl` pins the example's
checksum, and a test checks it against the file.
