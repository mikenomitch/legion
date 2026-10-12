# Judgement Calls: Authenticated Worker Plane and Isolated Workloads

Plan: [worker-plane-auth-plan.md](../plans/worker-plane-auth-plan.md)

Entries are ranked by project impact, highest first. An entry's number is its
rank. These were made while writing the plan, before any code, so each one is
also an open invitation to argue in review. Entries 1, 2, 4, 6, 7, 12, 16,
17, and 19 were added on 2026-10-11 when the plan took on local workload
isolation and folded in the sandbox-exec plan's amendments, which also
extended entry 14. Entry 19 reverses the plan's earlier call to keep a
plaintext path for one release. Entries 10, 11, 15, and 20 through 25 were
added on 2026-10-12 when Phase 1 shipped. They record where the code departs
from the plan and what Phase 1 deferred.

## 1. ACLs are on by default, and the first token is minted at first start

`acl.enabled` defaults to `true`. The manager mints the initial management
token itself at first start, writes it to `<dataDir>/bootstrap-token` and
(when absent) `~/.legion/token`, both `0600`, and the open `BootstrapAcl`
RPC is removed.

```text
INFO ACLs are on. Created the initial management token (accessor 7c1f...).
     Secret written to /var/lib/legion/bootstrap-token and ~/.legion/token (mode 0600).
```

**Alternatives.**
- (a) Keep ACLs off and bind the manager to a Unix socket for local use.
- (b) Keep ACLs off but require a token from non-loopback callers.
- (c) Keep ACLs off and deny allocation cgroups at the firewall.
- (d) Turn ACLs on but keep `legion acl bootstrap` as the way to get the
  first token, which is Nomad's and Consul's model.

**Trade-off.** Every API call now needs a token. Scripts off the manager's
machine need `LEGION_TOKEN`, the UI asks for a token on first visit, and every
test harness that assumed "off" has to say so. Infrastructure-as-code loses
the `--secret` flag and gains `acl.bootstrapSecretFile` instead.

**Why.** With ACLs off, any task on any node owns the cluster: co-located
tasks over loopback, and every other task over the network, because workers
must reach the manager anyway. (a) and (b) protect nothing in scope. A
host-network task is a loopback caller, the gated proxy dials from the
worker's loopback, and a root `exec` task can open any socket. (c) is in the
plan as defense in depth (§6.13), but it can't protect a remote manager, a
node without `nftables`, or macOS. (d) leaves an open endpoint that any local
task can race to after first start or after a recovery. Minting at first
start into a root-only file is what kubeadm and k3s do, and because the CLI
reads `~/.legion/token`, the laptop flow still has nothing to configure.
**Ease is preserved, and the default stops being a takeover.**

## 2. Linux containers are gated by default, with default-deny egress

`network.mode = "auto"` resolves to `gated` for `container` on Linux nodes.
A gated container with no policy denies egress with `gated-default`.
`network.mode = "host"` is the explicit opt-in and needs the `host` grant.

```nickel
network = { egress = { allow = ["api.stripe.com:443"] } },   # open one host
network = { mode = "host" },                                # opt out (host grant)
```

**Alternatives.**
- (a) **Bridge networking**: a veth pair and NAT per allocation, with
  `nftables` blocking the host and metadata. This is Docker's default and
  Nomad's opt-in `bridge` mode. UDP, ICMP, Java, and database drivers all keep
  working.
- (b) **Gated with allow-by-default**: a private namespace and the host
  guard, but egress open through the proxy unless a policy says otherwise.
- (c) Keep `host` as the default and rely on the kernel host guard (§6.13).

**Trade-off.** A container that dials out needs an `egress.allow` line.
No raw UDP, no ICMP, IPv4 only, and tools that ignore proxy variables (Java's
default client, most database drivers) have no route, so they need external
upstreams or `host`. Every container start pays for a namespace. Services
keep working only because Phase 3 also gives every gated allocation ingress
twins and a resolver.

**Why.** The gated-sandbox plan already argues for deny on every gated
runtime, because one mode with two defaults is the kind of thing nobody
remembers at 3am. (a) keeps egress policy cooperative and adds a second
datapath with IPAM next to gated, which is the "three concepts where we wanted
one" pattern Legion exists to avoid. The `bridge` mode name stays reserved for
the datapath plugin. (b) is one line away for any author who wants it
(`egress = { policy = "allow" }`), and the host guard holds under it, so the
default can be the strict one. (c) leaves every host daemon and every
non-root-but-hostile container one `connect()` from the node. **Safety and
one consistent concept won over compatibility.** Revisit when transparent
capture lands in gated namespaces, which removes the proxy-awareness cost.

## 3. Managers dial workers over mTLS, not a worker-initiated reverse channel

Workers keep a listener, and managers dial it with mutual TLS.

**Alternative.** Flip the direction, like Nomad's client connection or
Kubernetes' Konnectivity. Each worker holds a long-lived stream to a manager,
and the manager sends exec, logs, and dispatch down it. Workers would need no
inbound port, no serving certificate, and no advertised address, and would
work behind NAT.

**Trade-off.** Operators must open 4848 from managers to workers, and the
advertised address has to be right. NAT'd workers stay unsupported.

**Why.** Workers must be reachable anyway: the mesh dials node to node on task
ports, and edge boot has routers dial `StartClaim` directly. A reverse channel
would fix exec and logs for a NAT'd worker but leave its services
unreachable, so it buys a half-working node. It would also need inter-manager
routing in raft clusters (an exec that lands on manager B has to find the
manager holding the worker's stream), and it would replace a dispatch path
the benchmarks have tuned. mTLS is a transport change on paths that already
exist. **Simplicity and performance won over NAT support.**

```console
# what the operator does under this call
$ legion join lgnjoin_...   # plus: allow tcp/4848 from the managers
```

## 4. The host guard is a node-owned floor below policy

Loopback, the node's own addresses, link-local, and known metadata addresses
are denied for every allocation before policy runs. Only the node's operator
can open a hole, one `ip:port` at a time, in worker config. The same floor is
enforced in the datapath for gated tasks and by `nftables` for host-mode
tasks.

```nickel
hostGuard = { allow = ["10.0.1.7:9100"] },   # worker config, not job config
```

```text
Legion-Denied-By: host-guard
Legion-Denied-Reason: 169.254.169.254 is link-local (cloud metadata); allocations may not reach it
```

**Alternatives.**
- (a) Ship the floor as a stored `require` policy object that cluster admins
  can edit or delete, so it uses the existing policy machinery.
- (b) Let a job opt out (`network.hostAccess = true`) for the jobs that
  legitimately need a host service.

**Trade-off.** A job that legitimately needs a host service (a node exporter
scraper, a local license daemon) can't get it without the node's operator.
Two enforcement points (datapath and kernel) implement one rule, so they can
drift. A gated task can't reach a same-node service by the node's IP through
the proxy, only through an upstream.

**Why.** (a) puts the floor where a `net:write` mistake, or a compromised
admin token, removes it from every node at once, and policy merge rules
would have to treat it as special anyway so job rules can't widen it. (b)
turns the floor back into a default that a job author, the person the floor
protects against, can turn off. The thing at risk is the node, so the node's
config owns the exception. One name (`host-guard`) for both enforcement
points keeps the concept single for the person reading a denial. Shared
address-class code and one spot-check table keep them from drifting.
**Simplicity of the mental model won over flexibility.**

## 5. The worker binds all interfaces by default

Once every privileged RPC requires a manager certificate or a ticket, the
worker binds `0.0.0.0:4848` with no flag.

**Alternative.** Keep the loopback default and require
`LEGION_WORKER_HOST=0.0.0.0` for multi-host. Secure-by-default in the narrow
sense: nothing listens on the network unless asked.

**Trade-off.** A TLS listener is exposed on every worker by default. Its attack
surface is the TLS stack and the ticket verifiers for `StartClaim` and
direct `ExecTask`.

**Why.** The opt-in path is the bug we are fixing. `legion join` on a second
box would still silently break exec, logs, and the mesh until someone found
the env var. The listener is safe to expose because of 6.1, not because of the
bind, and a default that only works on one machine contradicts "adding a
worker is two commands." **Ease won.**

## 6. `exec` stays host-level, and gated `exec` refuses to run as root

`exec` keeps `network.mode = "host"` by default and keeps running as the
worker's user when `user` is unset. A gated `exec` task must name a non-root
`user`, or submit refuses it.

```text
Error: task "worker" sets network.mode = "gated" but runs as root.
A root exec task can leave its network namespace, so gated would not hold.
Hint: set user = "nobody" (or another unprivileged user), or use runtime = "container"
```

**Alternatives.**
- (a) Gate `exec` by default too, like containers.
- (b) Run `exec` as `nobody` by default, as Nomad's `exec` driver does.

**Trade-off.** A default `exec` task is still root on its node, and the
kernel host guard can't stop it (it can delete the table). The plan contains
that by permission (entry 7), not by isolation.

**Why.** `exec` exists to run host binaries with host paths and users. A
namespace around a root process is not a boundary, because it can `setns`
back, so (a) would add a default that breaks real `exec` jobs and still not
hold. (b) breaks every `exec` job that writes outside its task directory, and
Nomad itself pairs it with `raw_exec`, the unisolated runtime, disabled by
default. Legion's `exec` is that unisolated runtime, so the honest equivalent
is to make it a privilege and to say in the docs that untrusted code belongs
in `container` or `sandbox`. **Honesty and compatibility won over a default
that looks safer than it is.**

## 7. Host-level jobs need the `host` grant on `job:write`

The existing `host` flag, today only on `exec` rules, becomes valid on `job`
rules. Submitting a job with a host-level task (`exec`, a container with host
networking or relaxed security, a Linux `dev` sandbox) needs it.

```text
Error: PermissionDenied: job "backup" has a host-level task ("dump" uses runtime "exec").
Submitting it needs a job:write rule with host = true.
```

**Alternatives.**
- (a) A worker-config switch per runtime, like Nomad's `raw_exec` plugin
  `enabled = false`, so a pool can refuse host-level work.
- (b) A new ACL resource (`host`) instead of a flag.

**Trade-off.** Teams that deploy `exec` jobs today need their policies
updated. The check runs on the write path for every submit, after parsing.

**Why.** "Can deploy" meaning "is root on every node" is the gap. The
vocabulary already has `host` for exactly this distinction on exec, so
extending it to `job` adds no new concept, where (b) would add one. (a) is a
placement-time refusal that surfaces late and per node, and says nothing
about who may ask. A node-pool restriction is still available through a
`require` policy that imposes `mode = "gated"`. **Extensibility of the
existing ACL vocabulary won over a new knob.**

## 8. One cluster CA, with roles in URI SANs

The existing mesh CA signs manager certificates too. Roles live in the URI SAN
(`legion://manager/<id>`, `legion://node/<id>`), checked on every call.

**Alternative.** A separate control-plane CA, so a mesh CA rotation or
compromise cannot touch manager-to-worker trust.

**Trade-off.** Rotating the CA is now a control-plane event, which forces the
two-step rotation in plan 6.9. A leaked CA key compromises both planes at
once, but the key lives in the replicated store either way.

**Why.** Two CAs means two rotation procedures, two `net ca` stories, and two
trust bundles on every worker, to separate blast radii that share a storage
location. Nomad and Consul both run one CA with role names.
**Simplicity won.** Revisit if the CA ever moves to an external provider for
one plane but not the other.

## 9. The join token pins the CA hash

`legion join-token create` embeds `caHash = "sha256:..."`, and the worker
refuses a manager whose CA does not match.

**Alternative.** Trust on first use: accept whatever CA the manager presents at
first registration and pin it from then on. No token format change.

**Trade-off.** Rotating the CA invalidates the pin in outstanding tokens until
the overlap rules in 6.9 apply. The token gets about 80 characters longer.

**Why.** TOFU fails exactly when it matters: at first contact, which is when the
worker sends its join secret. The token is already the one secret the operator
moves between machines, so it is the natural place for the anchor. This is
kubeadm's `--discovery-token-ca-cert-hash`, folded into the token so there is
still nothing extra to type.

```console
error: the manager at https://10.0.0.5:4747 presented cluster CA sha256:51aa07...,
but this join token pins sha256:9c1e4f...
```

## 10. Phase 1 ships without the CA pin; the pin lands with the encrypted worker channel

Phase 1 shipped (2026-10-12) without §6.5. Join tokens stay version 1 with no
`caHash`, and a joining worker still trusts whatever answers at the token's
address. Phase 4, which encrypts the worker-to-manager channel, now carries
the pin.

**Alternative.** Ship the pin in Phase 1, as the plan's phase list says: token
v2 with a required `caHash`, and a worker that checks every CA it receives
against it.

**Trade-off.** Until Phase 4, an impostor at the token's address gets the
join secret and can hand the worker a CA of its own. The public docs
(`auth-and-acls.md`, "Known limits") say so.

**Why.** In Phase 1 the worker-to-manager channel is still plaintext, so the
first CA a worker could check arrives over that channel, *after* the join
secret has gone out. An impostor can return the real CA's PEM, which is public
(`legion net ca show`), and the pin passes. The pin protects the join secret
only when the worker checks it against the TLS handshake of the channel that
carries the secret, which is Phase 4's TLS-on-4747. Shipping it now would
break every outstanding token for a check that proves nothing. This does not
reopen entry 19: when the pin lands, version-1 tokens are refused outright.
**Simplicity won: one token break, when the pin means something.**

## 11. CA rotation stays one step, and a refused handshake makes a node re-issue

Phase 1 shipped without §6.9's two-step rotation. `legion net ca rotate`
still replaces the CA outright. A manager re-reads the CA every 10 seconds and
re-mints its own certificate when the CA changes. A node re-issues its
certificate on its next heartbeat after a handshake on its worker plane fails
for an unknown CA, rate-limited to once a minute. Its routine refresh, at
most 8 hours away, also re-issues it.

```text
INFO A worker-plane peer trusts a different cluster CA; re-issuing the node certificate on the next heartbeat
```

**Alternative.** The plan's overlap: publish a bundle of the old and new CAs,
keep signing with the old CA until every node has the bundle, then switch.
That needs a `next` CA and a phase on the `net_mesh_ca` row, a migration, and
`ca_pem` turning into a bundle on the wire.

**Trade-off.** The first exec, logs, or restart against each node after a
rotation fails. A node that nothing dials keeps its old certificate until its
routine refresh, and mesh hops between nodes on different CAs fail until
then, which is today's mesh behavior. The acceptance criterion "`ca rotate`
completes with no failed exec or logs call" is not met. Any client whose
handshake is refused for an unknown CA can trigger a re-issue, at most once a
minute per node.

**Why.** Rotation is a rare operator action, and Phase 1 removes the
8-hour outage the plan worried about: a node re-issues within one heartbeat of
the first refused dial. The overlap is a schema change in a store that four
other tracks were editing during the same release. It can land as its own
change without changing anything the user types. **Simplicity won, and the
gap is listed in the public docs.**

## 12. Per-allocation user namespaces and Docker's seccomp profile for Linux containers

Every Linux container gets its own 65,536-id host range, a cgroup namespace,
and Docker's default seccomp profile. Opting out of either needs the `host`
grant.

```nickel
config = { image = "postgres:17", command = ["postgres"],
           security = { userNamespace = true, seccomp = "default" } },   # the defaults
```

**Alternatives.**
- (a) One shared subordinate range for every container, as Docker's
  `userns-remap` does.
- (b) Require images to run as non-root, as Kubernetes' `restricted` profile
  does, and skip user namespaces.
- (c) gVisor (`runsc`) as the default runtime.
- (d) Widen capabilities to Docker's 14 now that user namespaces make them
  safer, for image compatibility.

**Trade-off.** Volumes need idmapped mounts, so older kernels and runtimes
fail volume-backed starts with a named reason. Files written into host
volumes before the change need re-owning once. The worker owns an id pool
and a slot table. Mounting `sysfs` in a user namespace forces an ordering
decision with the gated namespace that Phase 6 has to settle first.

**Why.** Today container root is host uid 0 with the whole syscall surface
open. (a) lets a container that escapes its mount namespace own every other
container's files. (b) breaks the large share of images that run as root,
and moves the burden to every job author. (c) costs syscall-heavy workloads
and compatibility, and Legion already ships the strong boundary for
untrusted code as the microVM `sandbox` runtime. (d) is not needed: the three
capabilities Legion keeps are already tighter than Docker's, and widening
them is a compatibility change this plan has no evidence for. Because the
worker extracts layers itself, shifting ownership costs nothing extra.
**Safety won, and the extraction path we already own kept it cheap.**

## 13. Workers send a CSR, departing from mesh_ca's manager-generated keys

The worker generates its key and sends a CSR. The manager no longer returns
private keys to anyone.

**Alternative.** Keep the current design (`src/manager/mesh_ca.rs:72-75`):
the manager generates both halves, which its own comment justifies as "a CSR
flow is more ceremony than a 24-hour credential warrants."

**Trade-off.** More code on both sides and a persisted key on the worker.

**Why.** That comment was right when the cert guarded only mesh flows. Now it
guards a node's identity to the control plane, and the channel it travels over
stays plaintext until Phase 4. A key that never leaves the node removes a
whole class of on-path attacks for the cost of one proto field. The old path
is removed rather than kept beside the new one (entry 19).

## 14. One listener with per-RPC caller classes, including tickets on `ExecTask`

One TLS listener. Manager-only RPCs check for a manager certificate,
`StartClaim` keeps ticket-only authorization, and `ExecTask` admits a manager
certificate or, when the worker sets `exec.direct = true`, an exec ticket. The
classification test allows more than one class per RPC.

```rust
("ExecTask", &[Caller::Manager, Caller::Ticket]),
```

**Alternatives.**
- (a) A second port for edge-boot routers and direct-exec clients, with
  mandatory client certs on the manager port.
- (b) A separate `DirectExec` RPC for ticket holders, so every RPC has
  exactly one class.

**Trade-off.** The TLS handshake succeeds for anyone. Authorization happens
per RPC, so a missing check on a new RPC would be a hole. The classification
test exists to catch that, and it now has to reason about RPCs with two
classes, so it also sends one request per class to prove the table matches
the handlers.

**Why.** One port per worker keeps firewall rules and docs simple, and
routers and exec clients benefit from TLS too, since they now know they
reached the real node. (b) would duplicate the exec session protocol for a
difference that is only in who may open it. The sandbox-exec plan asked for
the two-class shape (its §9), and folding it here keeps one table as the
source of truth. A request that carries a non-manager certificate is refused
rather than downgraded to the ticket path, so the two classes can't be mixed.
The per-RPC check mirrors how the manager already enforces ACLs, including
the "fail the build if unclassified" guard.

## 15. `ExecTask` admits only the manager until direct exec ships

The classification table lists `ExecTask` with one class, `manager`. The
`ticket` class (§6.1) and the "never downgraded" rule for two-class RPCs are
written into `authorize`, but no RPC uses them yet.

```rust
("ExecTask", &[Caller::Manager]),   // + Caller::Ticket with the sandbox-exec plan
```

**Alternative.** Implement the ticket class now, as the plan's Phase 1 bullet
lists it: exec tickets, `exec.direct`, and a JWKS verifier on every worker
that admits tickets.

**Trade-off.** The plan's acceptance line "lists `ExecTask` with two classes"
is not met. Every exec goes through a manager.

**Why.** Exec tickets (claims, minting, replay, revocation) belong to the
sandbox-exec plan, which is not built, and calls entry 16 makes direct exec
wait for Phase 4 anyway. A second class with no ticket behind it would be an
unreachable code path in a security check. The refusal text for the
two-class shape is already in the code and tested, so adding the class is
one table edit and a handler branch.

## 16. Ticket verifiers need the JWKS over TLS that pins the cluster CA

From Phase 4, a worker refuses to build a ticket verifier (edge boot or
direct exec) over a plaintext non-loopback manager address. Direct exec
therefore needs worker-plane-auth Phases 1 and 4.

```text
error: exec.direct = true needs the cluster JWKS over TLS, but the manager
address is http://10.0.0.5:4747
```

**Alternatives.**
- (a) Keep fetching over plain HTTP, as edge boot does today, and document
  the exposure.
- (b) Deliver the JWKS on the heartbeat reply.

**Trade-off.** Direct exec can't ship before the encrypted worker channel.
Edge boot keeps today's exposure until Phase 4 lands.

**Why.** A ticket is only as trustworthy as the keys that verify it. Over
plain HTTP, anyone on the path between a worker and its manager can swap the
JWKS and mint tickets that open shells. (b) rides the same plaintext channel,
so it moves the problem without fixing it. Refusing to run over plaintext
makes the dependency visible at start instead of hiding it in a doc.
**Safety won over shipping direct exec one phase earlier.**

## 17. The proxy judges resolved addresses and dials the address it judged

For a hostname, the proxy and the external-upstream dialer resolve first,
judge every address against the host guard and CIDR rules, and dial only an
address that passed. Name rules still match on the name.

**Alternatives.**
- (a) Keep judging names as names and rely on the kernel guard to catch
  bad addresses at dial time.
- (b) Resolve to judge, but let the dial resolve again.

**Trade-off.** Resolution moves before the verdict on every named
connection, so a slow resolver now delays the `403` as well as the connect.
The dialer has to take a resolved address and keep the original name for
SNI and the `Host` header.

**Why.** Today a CIDR rule never sees a hostname, so the documented metadata
rule is bypassed by any name that resolves to `169.254.169.254`, by
`localhost`, or by a decimal literal like `2130706433`. (a) doesn't work,
because the proxy dials from the worker, which the kernel guard deliberately
doesn't match. (b) leaves a DNS-rebinding window between the verdict and the
dial. Resolution already happens on every named dial, so judging its result
adds no lookup. **Correctness won, at no performance cost.**

## 18. The advertise address is auto-detected from the route to the manager

The worker advertises the local address it uses to reach its manager, unless
`LEGION_WORKER_ADVERTISE_ADDR` or `--advertise-addr` says otherwise.

**Alternative.** Require the address explicitly, or advertise the first
non-loopback interface.

**Trade-off.** Behind NAT, or with asymmetric routing, the guess is wrong and
only the Phase 8 probe tells you.

**Why.** On the common topology (managers and workers on one private network)
the source address toward the manager is exactly the address the manager
should dial back, and a co-located worker still gets `127.0.0.1`. "First
non-loopback interface" picks Docker bridges and VPN tunnels. Requiring a flag
breaks the two-command join.

## 19. No compatibility paths: no plaintext workers, no unpinned tokens, no server-generated keys

A new manager never dials a worker over plaintext, join tokens without
`caHash` are refused, and the node certificate API takes a CSR only. A
protocol-2 worker keeps heartbeating, but exec, logs, and restart against it
fail with an error naming its protocol version.

This reverses the plan's earlier call, which kept plaintext to a protocol-2
worker on loopback for one release, accepted unpinned tokens with a warning,
and kept manager-generated keys for old workers.

**Alternative.** That earlier call: keep each old path for one release so a
rolling upgrade is invisible.

**Trade-off.** During an upgrade, exec and logs against a not-yet-upgraded
worker fail until it is upgraded. Outstanding join tokens must be re-minted.

**Why.** AGENTS.md says Legion has no installed base to protect and asks for
no deprecation shims or dual code paths. Each kept path was a branch in a
security-sensitive factory, plus tests, to preserve setups that only ever
worked on one machine. Dispatch still reaches old workers through heartbeat
replies, so running work is never interrupted, only interactive calls.
**Simplicity won.**

## 20. Worker-plane channels are keyed by (node name, address), not by address

The manager's factory caches one mTLS channel per `<node>.node.legion@<address>`.
Each channel's TLS config names the node it verifies, so two nodes that
advertise one address get two channels.

**Alternatives.**
- (a) Key by address, as the old dispatcher cache did.
- (b) Key by node id.

**Trade-off.** There are a few more channels when real nodes share an
address (NAT, misconfiguration). A rotated CA or a re-minted manager
certificate drops the whole cache, because both are baked into each channel,
so the next call to each node pays one handshake.

**Why.** (a) would send node B's payload over a channel that verified node A,
which is the stolen-address case §6.3's name check exists to stop. (b) would
open one TLS connection per simulated node to the same listener: 5,000
connections for a 5,000-node sim process. Keying on what the channel verifies
is correct and keeps the simulator at one connection per process.
**Correctness won, at no performance cost on real fleets** (one node, one
address, one channel, as before).

## 21. A simulated fleet serves one certificate, named by `workerPlaneName`

This takes the lean of the plan's open question 4. The fleet's first node
(by name) requests a normal node certificate after it registers. The
process's one listener serves that certificate. Every node in the process
registers `workerPlaneName = "<that node>"`, and managers verify that name
instead of the node's own, but only on nodes that also report
`simulated: true`.

```json
{ "simulated": true, "workerAddress": "https://10.0.1.17:41233", "workerPlaneName": "sim-a-util-0" }
```

**Alternatives.**
- (a) One certificate with a SAN per simulated node, which would be 5,000
  SANs.
- (b) One listener per simulated node.
- (c) Let any node name the certificate that serves it.

**Trade-off.** A node that reports `simulated: true` can point the manager's
name check, for its own dials, at another node's certificate. The worst this
does is redirect that node's own payloads to a peer it names. It never grants
manager access. Push dispatch to a fleet waits until the identity node has
registered, and heartbeats deliver work in the meantime.

**Why.** (a) and (b) give up what makes the simulator cheap: one listener
and a handful of connections per process. (c) would make every real worker's
name check optional. Scoping the override to simulated nodes keeps real
fleets on per-node verification. **Performance of the benchmark rig won,
with the exception fenced to nodes that say they are fake.**

## 22. No reachability probe yet: the failing call explains itself

Phase 8 (`Worker/Probe`, the node-record field, `WORKER_UNREACHABLE`) is not
built. Instead, every manager-to-worker failure that never reached the
worker goes through one explainer. A worker's own answer passes through
with its code.

```text
cannot reach node wkr-1 at https://10.0.1.7:4848 (tcp connect error: Connection refused (os error 111)).
Managers dial workers on port 4848: allow it from every manager, or set
LEGION_WORKER_ADVERTISE_ADDR on the worker if this address is wrong
```

The dial now times out after 5 seconds instead of waiting for the kernel's
TCP SYN timeout.

**Alternative.** Ship the probe in Phase 1, so a firewalled worker shows up
in `legion node info` before anyone needs a shell.

**Trade-off.** A firewalled worker still looks healthy until the first exec,
logs, or restart against it. The error is `Unavailable` (retryable) with the
hint, not the typed `WORKER_UNREACHABLE` envelope code.

**Why.** The spot check's complaint was a bare `transport error` with no
node, address, or port. This fixes that on the path where it hurts. The probe
needs a new RPC, a node-record field, CLI and UI output, and an envelope
code in the CLI's error mapping, which is another track's file in this
release. It stays Phase 8.

## 23. The worker plane listens only once it holds a certificate, and asks for a fresh one at every registration

The worker registers its `https://` address, then requests a node
certificate (bounded at 10 seconds), and binds 4848 only after a certificate
is installed. A certificate persisted under `<state-dir>/identity/` is served
right away on a restart, and the worker still requests a fresh one at every
registration.

**Alternatives.**
- (a) Bind at startup and refuse handshakes until a certificate arrives.
- (b) Reuse a persisted certificate until its routine refresh.

**Trade-off.** A manager dial in the window between registration and
issuance gets "connection refused". Push dispatch then falls back to the
heartbeat, and exec gets the unreachable message. Each worker start costs one
CSR round trip.

**Why.** (a) has nothing to present, so it would log a "listening" line that
is not true. (b) would keep serving a certificate from a rebuilt cluster or a
rotated CA after a restart until the first refused dial. One CSR per process
start is noise.

## 24. A manager mints its certificate on first dial, not at startup

§6.2 says each manager mints its certificate at startup. Instead the factory
mints it on the first manager-to-worker call, from the CA it re-reads every
10 seconds, and re-mints after 8 hours, or when the CA changes.

**Alternative.** Mint at startup, as the plan says, with a retry loop.

**Trade-off.** The first exec or dispatch after a manager starts pays one
ECDSA keygen and signature (under a millisecond). A broken CA surfaces on
first use, not at boot.

**Why.** A raft manager starts before its group has a leader. A fresh
cluster has no CA until the first worker asks for a certificate, and creating
one is a replicated write. Startup minting would need a retry loop in
manager startup code that the HA work was reshaping in the same release. On
demand, the certificate exists whenever there is something to dial, and a
rotated CA is handled by the same path.

## 25. One `--advertise-addr` for real and simulated workers; the simulator's `--advertise-host` is gone

`legion join` and `legion start worker` take `--advertise-addr HOST[:PORT]`,
which wins over `LEGION_WORKER_ADVERTISE_ADDR`. The simulator's
`--advertise-host` (default `127.0.0.1`) is removed. Simulated fleets take the
same flag, and by default they detect the address the way a real worker does.

```bash
legion start worker $TOKEN --simulate 1000 --advertise-addr 10.0.1.17   # was --advertise-host
```

**Alternative.** Keep `--advertise-host` for the simulator next to the new
flag.

**Trade-off.** Benchmark scripts that pass `--advertise-host` break. The one
in this repo (`benchmarks/aws/scripts/start-sim-fleet.sh`) was updated.

**Why.** Two flags with one meaning is the drift AGENTS.md asks us not to
keep ("no compatibility aliases"). The CLI records the flag in a
process-wide setting rather than adding a parameter to `run_worker`, whose
signature the manager-failover work was changing in the same release.
