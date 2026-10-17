# Judgement Calls: Egress Plugins Proposal

Calls behind [`plans/egress-plugins-proposal.md`](../plans/egress-plugins-proposal.md).
Entries are ranked by project impact, highest first; an entry's number is its
rank. The proposal is not built, so these are the calls the proposal makes and
would ask the owner to ratify; each says what evidence would overturn it.

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

**Trade-off.** Two plugin models to document instead of one, and two
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

## 2. Job authors never load kernel code; their rules compile to maps

**Call.** The kernel tier runs one fixed, Legion-owned program set; a job
author's (or a policy object's) rules are compiled into map entries.
Operator-written eBPF programs (proposal C) exist, are policy-objects-only,
need unscoped `net:write`, and come last.

```
egress.handlers[1] plugin "no-smtp" is an ebpf program, and ebpf programs
run only from policy objects. Ask an operator to apply it with a policy
that selects this job.
```

**Alternatives.** (a) Let job authors ship eBPF programs for their own
tasks, verified by the kernel. (b) A small expression language
(`dst.port in [25, 465] && labels.tier == "free"`) that Legion compiles to
BPF bytecode, so authors get logic without writing eBPF.

**Trade-off.** Kernel-speed custom logic is an operator privilege; a job
author with a connect-time need that rules cannot express goes through a
WASM `connect`-stage plugin, which forces a redirect and costs a hop.

**Why (security first, then simplicity).** The verifier is the only thing
between a tenant's program and the kernel, and verifier bugs have been
privilege escalations — the bpfman model (privileged daemon, RBAC-gated
program objects) is the industry answer for the same reason. (b) is safe
but is a second policy language with a compiler whose output must match
`evaluate_connect` exactly; rules plus selectors already express every
scenario in the proposal.

---

## 3. Allowed flows that nothing reads pass without entering netd

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

## 4. Gated stays a namespace; the kernel tier adds transparency, not a new boundary

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

## 5. Hostnames reach the kernel through the resolver; DNS is captured

**Call.** All DNS from governed tasks is rewritten to the allocation's
resolver; allowed answers write their addresses (with a TTL floor) into the
allocation's learned-address map; a connection to an address the resolver
never returned is redirected and judged by SNI or `Host`, with the dialed
address checked against that name's own resolution.

**Alternatives.** (a) Peek SNI on every TLS flow under a hostname rule — no
DNS dependency, but every such flow enters netd, which undoes entry 3.
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

## 6. Plugins use the proxy-wasm ABI, not a Legion ABI or the component model

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
feature (signing with credential handles, entry 7, is the first test).

---

## 7. Plugins never hold credentials; signing is a host function over a handle

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

## 8. Handler entries become keyed variants, breaking the flat form

**Call.** `{ handler = "authz", url = ... }` becomes
`{ authz = { url = ... } }`, with `timeout` and `onError` on the entry.

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

## 9. A plugin is referenced by name; its kind lives on the object

**Call.** Chains say `{ plugin = "llm-token-meter" }`; whether that is WASM
or eBPF, which stages it runs at, and who may use it are properties of the
stored plugin object.

**Alternatives.** Key the reference by mechanism —
`{ wasm = "llm-token-meter" }`, `{ ebpf = "no-smtp" }` — so a reviewer sees
the cost class in the chain itself, as `protocol` is never inferred.

**Trade-off.** Reading a chain alone no longer tells you where each hop
runs; `show`/`doctor` have to (§6.E.4), and stage mismatches are caught at
submit rather than by eye.

**Why (extensibility).** An operator can replace a WASM plugin with an
eBPF program of the same name (or the reverse) without editing every job
that references it, and the plugin object is the one place its trust level
(`usableBy`) and limits are reviewed. The "readable cost" principle is kept
by printing placement, not by encoding it in the reference.

---

## 10. Transparent capture is a connect-time rewrite plus a map

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
