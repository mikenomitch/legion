# Judgement Calls: Mesh Interfaces Plan

Calls behind [`plans/mesh-interfaces-plan.md`](../plans/mesh-interfaces-plan.md).
Entries are ranked by project impact, highest first; an entry's number is its
rank. Nothing in the plan is built, so these are the calls the plan makes and
asks the owner to ratify. Each says what evidence would overturn it.

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

## 8. The mesh is a switch on the service; TLS lives on ports; chains carry only policy

**Call.** `service.mesh = true` replaces
`ingress.tls = { mode = "mutual", certFrom = "legion", clientCa = "legion" }`.
Non-mesh TLS moves to `network.ports[].tls`, and an external upstream's trust
bundle to the upstream's `ca`. `tls` is removed from chains.

```nickel
# before
network = { ingress = { tls = { mode = "mutual", certFrom = "legion", clientCa = "legion" } } },
# after
service = { port = "http", mesh = true },
```

**Alternatives.** (a) Keep TLS on chains, with a `tls = "mesh"` shorthand.
That is the smallest change. (b) A task-level `network.mesh` switch that
covers every port.

**Trade-off.** A policy object can no longer push certificate material onto
the tasks it matches, which `certFrom = { kv }` on a policy object does
today. A platform team that did that has to move certificates into the jobs.
A policy object can still impose `mesh = true`.

**Why (simplicity).** A chain is policy: what may pass. TLS is how the port
is served, and it belongs with the port, as Gateway API puts
`BackendTLSPolicy` on the backend rather than the route. With (a), TLS
modes would still mean different things on ingress and egress. (b) is too
coarse: a task can serve a public `https` port and a mesh service port at
once.

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
Require `db.svc.legion` for catalog targets (open question 8).

**Trade-off.** The rule misreads a dotless intranet host (`http://wiki`),
which must be written with its port. It also needs a clear submit error when
a bare name is not a valid service name.

**Why (simplicity).** The jobspec reads like the URL the task dials, the
scheme carries the protocol (entry 6), and there is one validation path
instead of a matrix. Nomad's upstream is one shape. (b) invents a scheme no
client understands, and (c) is the honest alternative, kept open.

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

## 17. Seven knobs and kinds are removed rather than reshaped

**Call.** The plan removes the `log` handler, chain-level `onError`,
`proxy.node`, `http.outlier`, `retries.on` and `retries.methods`, the
`random` and `least_conn` balancers, and the reserved `bridge`/`cni/*`
modes (plan §6.0).

```nickel
# before
http = { retries = { attempts = 2, on = ["5xx", "reset"], methods = ["GET"] },
         outlier = { consecutive5xx = 5, ejectFor = "30s" } },
balance = "least_conn",
# after
http = { retries = { attempts = 2 } },   # ejection is always on in HTTP mode
balance = "least_latency",               # the default; least_conn's behavior is inside it
```

**Alternatives.** Keep every knob and reshape only the confusing ones.

**Trade-off.** A user who tuned ejection, retried on `reset` or
`retriable-4xx`, retried a non-idempotent method, or relied on `least_conn`'s
exact tie-breaking loses that. Each is one registry row or field to bring
back, and a concrete request for one is the evidence to do so (open
question 11).

**Why (simplicity).** Each removal either duplicates something that stays
(`log` is `accessLog`, `node` is `service` + `local`, `least_conn` is inside
`least_latency`, outlier ejection overlaps `least_latency`'s failure
penalty) or does not work today (chain `onError`), or is a reserved name for
a feature that does not exist. The owner's ask was a mesh without the
kitchen sink. Removing these is cheaper now, with no installed base, than at
any later point.

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
