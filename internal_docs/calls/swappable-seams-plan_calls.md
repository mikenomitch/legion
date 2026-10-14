# Judgement calls: swappable seams

Plan: [`plans/swappable-seams-plan.md`](../plans/swappable-seams-plan.md).
The cluster CA half records its own calls in
[`cluster-ca-provider-plan_calls.md`](cluster-ca-provider-plan_calls.md).

Entries are ranked by project impact, highest first; the number is the rank.

---

## 1. The attachment is a closed enum, not a trait or a plugin kind

**Call.** How an allocation reaches the datapath is
`netd::attach::Attachment` — `Host`, `Netns`, `GuestVsock`, `GuestTap` —
decided once per allocation and carried by every listener spec. Adding a
capture mode is a new variant and its arms in `listen` and `task_dial`, in
this crate.

**Alternatives.** (a) A `Capture` trait with the four modes as
implementations, so an out-of-tree crate could add one. (b) A datapath
plugin kind (`plugins/datapath/`), the CNI-shaped answer the service-discovery
plan once sketched.

**Trade-off.** Nobody outside the repository can add a capture mode. A CNI
bridge, TPROXY, or a TUN device is still a Legion change, reviewed here.

**Why (extensibility, kept deliberately closed).** The audit's problem was
never that modes could not be added from outside; it was that adding one
inside meant finding ~40 sites. A closed enum turns that into a compile
error at every `match` that misses the new variant, which a trait object
does not. Service-mesh calls entry 5 keeps the datapath plugin deferred, and
nothing here reopens it; the enum is the seam that makes that plugin cheap
when it is wanted, because `Netns { path }` is already the CNI contract.

---

## 2. The worker ↔ datapath boundary is an in-process enum, with no wire encoding yet

**Call.** The worker talks to the datapath through `Netd` and three types:
`NetdInput` (an enum of everything it tells it), `NetdWants` (what to fetch
from the manager), and `NetdReport` (what goes on the next heartbeat).
`NetdInput` derives no `Serialize`; the persisted state is `DesiredNetwork`
plus policy bodies, as before. `ForwarderContext` stays one internal type
behind `Netd`.

**Alternatives.** (a) Ship the `legion netd` process split now, with a Unix
socket protocol. (b) Make `NetdInput` serializable today so the split is
only a transport. (c) A `Datapath` trait with an in-process implementation
and a future client implementation.

**Trade-off.** The split is still work: a transport, framing, supervision,
fd hand-off. Secrets (credentials, KV-held certificates, the mesh key) ride
in `NetdInput` variants, and whoever adds the wire encoding must keep them
off disk — `netd_state.json` must keep holding only `DesiredNetwork` and
bodies. `ForwarderContext` is still large; splitting it is open in TODO.

**Why (simplicity now, extensibility kept).** The split's hard part was
knowing what crosses the boundary; that is now a type the compiler checks.
Serializing it before there is a second process would add derives on
secret-bearing types with nobody to read them, and a trait with one
implementation is ceremony. Service-mesh calls entry 17 deferred the split
for good reasons that still hold.

---

## 3. A node whose `resolv.conf` names no nameserver answers `SERVFAIL`, not `1.1.1.1`

**Call.** The per-allocation resolvers forward only to the `nameserver`
lines of `/etc/resolv.conf`. With none, every name outside `svc.legion` gets
`SERVFAIL` at once, and the worker warns at start:

```text
WARN /etc/resolv.conf names no nameserver; allocation resolvers will answer SERVFAIL for every name outside svc.legion
```

Before, the resolver silently forwarded to `1.1.1.1:53`.

**Alternatives.** (a) Keep the fallback and document it. (b) A worker config
field for upstream resolvers, with `1.1.1.1` as its default.

**Trade-off.** A node with an empty `resolv.conf` that "worked" before (by
leaking queries to Cloudflare) now resolves nothing outside the zone until
the operator names a resolver.

**Why (simplicity, and not surprising the operator).** Sending a tenant's
queries to a third party nobody configured is a privacy and egress surprise
on exactly the nodes (air-gapped, locked-down) where it matters most.
`SERVFAIL` fails fast and the warning names the cause. A config field can
come when someone asks for resolvers different from the host's; there is
no installed base to protect (AGENTS.md).

---

## 4. The egress proxy keeps connections alive and judges every request

**Call.** HTTP through the gated egress proxy is served by hyper. Each
request on a client connection — kept alive or pipelined — is judged on its
own and sent upstream on a connection of its own, in origin form.

**Alternatives.** Keep one request per client connection: answer the first,
send `Connection: close`, and refuse anything pipelined behind it. That is
what the hand-rolled parser intended, and it is simpler to reason about.

**Trade-off.** A client connection now lives as long as the client wants,
so the activity ledger counts it open longer (an idle keep-alive connection
to the proxy keeps a `connections`-signal idle policy awake). The upstream
side still opens a connection per request, so there is no connection reuse
to the origin.

**Why (security, then ease).** The old parser judged the first request and
then spliced the rest of the connection, so a pipelined second request
reached the origin unjudged; only the origin honouring `Connection: close`
stood in the way. Judging per request removes the dependence on the origin.
Keeping the client connection is what every HTTP client expects of a
forward proxy, and refusing it would make pip and npm slower for nothing.

---

## 5. Policy assignments land with the desired set, before templates and fetches

**Call.** `NetdInput::Desired` applies the listeners, the endpoint feed, the
attribution identities, and the policy assignments together. Before, the
heartbeat applied assignments later in the tick, after templates and live KV
files were written.

**Alternatives.** Keep assignments a separate input sent where the old code
applied them.

**Trade-off.** One heartbeat tick does the policy work earlier. Nothing in
templates or KV files reads policy, so no behavior depends on the old order.

**Why (simplicity).** A listener bound in the same step as its policy
assignment sees the assignment immediately, so a gated listener spends less
time failing closed on `policy-pending`. One input for "what the manager
said this beat" is easier to carry over a socket later than two.

---

## 6. A truncated answer is refetched over TCP only for clients that asked over TCP

**Call.** When a forwarded answer comes back truncated, the resolver refetches
it over TCP only if the client reached it on its TCP listener. A UDP client,
and a gated sandbox guest (whose agent relays UDP queries over a vsock
stream), gets the truncated answer.

**Alternatives.** Always refetch over TCP and return the full answer.

**Trade-off.** A guest's large answer still costs it a retry over TCP to its
own agent, which relays that as a stream too.

**Why (correctness).** A full answer handed back for a UDP client can exceed
the buffer it advertised and be dropped. Answering each transport the way
its client expects is what every resolver does; the guest case is a UDP
client in disguise.

---

## 7. A node that reported no `os` gets no VIP block

**Call.** Placement assigns a VIP block only when the node's `os`
fingerprint is `linux`. A node with no `os` attribute gets none, and its
upstreams keep their loopback ports.

**Alternatives.** Fall back to the manager's own OS when the node reported
none (the old behavior, everywhere).

**Trade-off.** A Linux node whose fingerprint lacks `os` (a simulated node,
or a custom fingerprinter) loses name-is-config until it reports one.

**Why (correctness).** The bug was answering a node question with a manager
fact; keeping that as a fallback keeps the bug for the case it is hardest to
spot. Every real worker reports `os` (`std::env::consts::OS`).

---

## 8. `hickory-proto` 0.25 with only `std`, for parsing and building

**Call.** The resolver uses `hickory-proto` 0.25.2, default features off,
`std` on, to parse queries and build answers. Forwarding stays our own code
(UDP, and TCP for refetches).

**Alternatives.** 0.26 (the latest); or `hickory-resolver` to do the
forwarding as well.

**Trade-off.** One minor version behind. Forwarding, timeouts and resolver
order stay ours to maintain.

**Why (simplicity).** The parser was the part worth replacing: compressed
names, EDNS, and response building are where hand-rolled DNS goes wrong.
The forwarding is thirty lines whose semantics (first answer wins, policy
before forwarding) we want explicit. 0.25's builder API is the one the code
was written and tested against.

---

## 9. `dial_tcp` sets no socket options

**Call.** `netd::listen::dial_tcp` is connect-with-timeout and nothing else.
Callers keep the options they set before (`set_nodelay` on HTTP-mode and
ingress dials, nothing on L4 splices).

**Alternatives.** Set `TCP_NODELAY` on every datapath dial in one place.

**Trade-off.** The seam does less than it could.

**Why (scope).** A behavior change on every spliced connection belongs with
the datapath performance work in flight
([#42](https://github.com/mikenomitch/legion/pull/42)), not in a refactor.
The seam is where such a change, or a future capture mode's `SO_MARK`, will
go.

---

## 10. Restore seeds the namespace set; templates add their own problems

**Call.** `Netd::restore` runs the same `Desired` path as a heartbeat, so the
gated namespaces of surviving allocations enter the set the datapath tears
down from; before, a restarted worker started with an empty set and never
removed those namespaces when their allocations left. Template-render
problems, which the datapath does not know about, are appended to each
status by the worker after `report()`.

**Alternatives.** Keep restore separate from capture; make `Netd` call into
the templates module.

**Trade-off.** Namespaces of allocations that did *not* survive a restart are
still not torn down (the restore only knows survivors); that leak predates
this work and is an open item. Template problems now come last in an
allocation's problem list instead of before credential problems.

**Why (simplicity).** One path for "here is what should exist" is easier to
keep correct than two, and the datapath should not depend on the worker's
template renderer.
