# Judgement Calls: Authenticated Worker Plane and Isolated Workloads

Plan: [worker-plane-auth-plan.md](../plans/worker-plane-auth-plan.md)

Entries are ranked by project impact, highest first. An entry's number is its
rank. These were made while writing the plan, before any code, so each one is
also an open invitation to argue in review. Entries 1, 2, 4, 6, 7, 10, 13,
14, and 16 were added on 2026-10-11 when the plan took on local workload
isolation and folded in the sandbox-exec plan's amendments, which also
extended entry 12. Entry 16 reverses the plan's earlier call to keep a
plaintext path for one release.

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

## 10. Per-allocation user namespaces and Docker's seccomp profile for Linux containers

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

## 11. Workers send a CSR, departing from mesh_ca's manager-generated keys

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
is removed rather than kept beside the new one (entry 16).

## 12. One listener with per-RPC caller classes, including tickets on `ExecTask`

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

## 13. Ticket verifiers need the JWKS over TLS that pins the cluster CA

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

## 14. The proxy judges resolved addresses and dials the address it judged

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

## 15. The advertise address is auto-detected from the route to the manager

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

## 16. No compatibility paths: no plaintext workers, no unpinned tokens, no server-generated keys

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

## 17. A call with no credential is `Unauthenticated`, not `PermissionDenied`

Made while implementing Phase 0. When ACLs deny an anonymous caller on a
protected endpoint, the manager now answers `Unauthenticated` with a hint that
names the token file, instead of `PermissionDenied`:

```text
$ legion job list -j
{"ok":false,"error":{"code":"Unauthenticated",
 "message":"Failed to list jobs: job:read requires a token, and no token was provided",
 "hint":"set LEGION_TOKEN or run `legion login`. On the manager's machine, the initial management token is in <dataDir>/bootstrap-token and ~/.legion/token",
 "retryable":false}}
```

**Alternative.** Keep `PermissionDenied` for every denial, as before, and
only append the hint.

**Trade-off.** Clients that matched `PermissionDenied` to detect "not logged
in" now see a different code. The `acl_endpoints` matrix and one anonymous
policy test changed with it. The UI already treats both codes as "log in".

**Why.** With ACLs on by default, "no token at all" becomes the first error
every new user and every agent hits, so it has to be unambiguous. gRPC's
`Unauthenticated` means exactly that, and an agent can branch on the code
(re-authenticate) instead of parsing prose. `PermissionDenied` keeps its
meaning: a known caller whose policies fall short. The plan's Phase 0
acceptance criterion already said `Unauthenticated`. **Ease for agents won.**

## 18. The bootstrap secret is on disk before the replicated insert

Made while implementing Phase 0. The manager writes the secret to
`<dataDir>/bootstrap-token.pending`, then makes the replicated
"insert if no management token exists" write, then renames the file into
place. A start that finds a pending file finishes the rename when the secret
matches a stored management token, and deletes it otherwise.

**Alternative.** Insert first, then write the file, which is simpler.

**Trade-off.** One more file name and a recovery step at startup.

**Why.** With insert-first, a crash between the two steps leaves a cluster
whose only management token's secret is in no file, and with no bootstrap
endpoint the only way back is deleting rows in SQLite by hand. Writing first
makes that crash harmless, and the replicated insert stays the single point
that decides who wins. **Simplicity for the operator won over a few lines of
code.**

## 19. An existing `~/.legion/token` is kept, with a warning that names the fix

Made while implementing Phase 0. The plan writes `~/.legion/token` only when
it does not exist. The implementation also logs a `WARN` when it was kept:

```text
WARN /home/me/.legion/token already exists and was left alone, so the CLI on
this machine keeps using the token in it. To use the new management token:
export LEGION_TOKEN="$(cat .legion/bootstrap-token)"
```

**Alternative.** Overwrite the file whenever a new cluster mints its token,
since on a laptop the existing file usually belongs to a cluster that was
deleted (`mise run nuke`).

**Trade-off.** A developer who wipes and restarts a local cluster gets
`Unauthenticated` from the CLI until they export the new token or delete the
stale file. The warning and the error hint both point at the fix.

**Why.** The file may hold a login to a different, live cluster, and
silently replacing someone's credential is worse than one confusing error
with a hint. The manager can't tell a stale token from a live one for another
cluster. **Safety won over one step of convenience.**

## 20. CA rotation cross-signs the new CA and switches signing at once

Made while implementing Phase 1, and a deviation from plan §6.9. The plan
kept signing with the old CA "until every node has fetched the bundle", then
switched. The implementation generates the new CA, cross-signs it with the
old one, and signs with the new CA immediately. Leaves carry the cross
certificate in their chain during a 24-hour overlap, and the trust bundle
holds both CAs for that overlap.

```console
$ legion net ca rotate
Rotated the cluster CA. The new CA (pin sha256:...) signs every certificate from now on.
The previous CA stays trusted until 2026-11-03T18:05:11Z, so no worker-plane or mesh connection breaks;
```

**Alternative.** The plan's three steps: publish a bundle, track which nodes
have fetched it, switch signing once every live node has, then drop the old
CA 24 hours later.

**Trade-off.** Chains grow by one certificate during the overlap. A worker
that is offline for the whole overlap can't verify the new CA when it comes
back and must re-join. The plan's version would have stranded it too, once
the old CA dropped.

**Why.** "Every node has fetched" needs per-node acknowledgement state on the
control lane and a decision about nodes that never come back. Cross-signing is
how public CAs roll their roots: a holder that trusts either CA verifies
either chain, so nothing has to be tracked. The heartbeat reply now names the
signing CA's pin (`HeartbeatReply.cluster_ca_pin`), so nodes re-issue within
one beat rather than within 8 hours. **Simplicity won, with no connection
broken during the overlap.**

## 21. The simulator's fleet certificate is requested through its first node

Made while implementing Phase 1, answering open question 4. The simulator
serves every simulated node from one listener, so it holds one certificate
(`<prefix>.fleet.legion`, `legion://fleet/<prefix>`). It asks for it with
`IssueNodeCertificate { fleet = "<prefix>" }` using the first registered
node's token. The manager signs a fleet certificate only for a node whose
registration carried `workerFleet = "<prefix>"`, and verifies the fleet name
when it dials any node with that attribute.

**Alternative.** One certificate with a SAN per node (5,000 names for a
5,000-node fleet), or a plaintext stub listener for the simulator.

**Trade-off.** A real worker could register with `workerFleet` set and get a
fleet certificate. That gains it nothing: it can change only its own node
record, so it can only receive its own work. The simulator's listener starts
a moment after its first node registers, and a push before then waits one
heartbeat.

**Why.** The benchmark rig has to measure dispatch over the same mTLS the real
fleet pays for. A plaintext stub would hide the handshake cost the plan asks
to measure, and a 5,000-SAN certificate is absurd to sign and to parse on
every handshake. **Fidelity of the benchmark won.**

## 22. The host guard judges `proxy` handler targets too, so a node-local proxy needs an `allow` entry

Made while implementing Phase 2. The plan names the egress proxy and external
upstreams. The datapath also dials `proxy` handler targets, and a policy
object's handler can name any `host:port`, so those dials go through the same
resolve-judge-dial step. A handler pointing at a node-local sidecar
(`target = "127.0.0.1:15001"`) is refused until the operator opens it:

```nickel
hostGuard = { allow = ["127.0.0.1:15001"] },
```

Handler health (`legion net doctor`, the heartbeat report) shows the refusal
as `host-guard: 127.0.0.1 is loopback; allocations may not reach it`.

**Alternative.** Trust handler targets, since policy objects are written by
`net:write` holders, not by the allocation.

**Trade-off.** A cluster that runs a node-local proxy hop breaks until each
worker type lists it, and the `allow` entry is per worker type, not per
policy.

**Why.** An inline policy is part of the job, so "written by the operator" is
not guaranteed, and a hop the allocation's traffic is handed to is still a
connection made on its behalf. One rule for every dial is simpler to reason
about than a list of trusted paths. **Simplicity of the boundary won** over a
one-line config change for the clusters that need it.

## 23. `authz` calls and the node's own traffic are not judged

Made while implementing Phase 2. `authz` handler calls, artifact fetches, and
the worker's calls to the manager go out without the host guard. An `authz`
call carries request facts (method, path, headers the policy forwards), never
task bytes, and its answer is a verdict the task never sees.

**Alternative.** Judge `authz` URLs as well, refusing loopback and
link-local webhooks.

**Trade-off.** A policy author can make the worker POST request facts to a
link-local or loopback address. They can't read the answer, and IMDSv2's
token call is a `PUT`.

**Why.** The common `authz` deployment is a node-local or loopback service, so
judging it would break the main use for little gain. The limit is in
`docs/network-handlers.md` ("The host guard", Limits). **Ease of the common
setup won.**

## 24. The listener waits for its certificate; registration and heartbeats do not

Made while implementing Phase 1. Issuance is eager (right after
registration), but a failed first issuance is not fatal unless the CA fails
the join token's pin. The worker keeps heartbeating and running work, retries
on every heartbeat, and the worker plane listener starts the moment a
certificate (issued or persisted) is held.

**Alternative.** Refuse to finish joining until the certificate is issued.

**Trade-off.** For a few heartbeats after a manager hiccup, a worker can be
registered without serving exec or logs. Dispatch survives: heartbeats carry
the work.

**Why.** A worker that crash-loops because a manager blinked during
registration is worse than one that serves its plane a few seconds late. The
pin mismatch stays fatal because it is the one failure where continuing would
mean trusting an impostor. **Availability won where it costs no trust.**

## 25. Several workers on one host need `--advertise-addr` to be told apart

Found while implementing Phase 1. The plan's integration test runs two
workers in one process on `127.0.0.2` and `127.0.0.3`, relying on
auto-detection. On Linux the source address of the route to any `127/8`
manager is `127.0.0.1`, so both would advertise `127.0.0.1`. The end-to-end
test (`tests/worker_plane_e2e.rs`) runs one real worker through the binary
instead, and the docs say co-located workers need their own port and, to be
distinguished by address, `--advertise-addr`.

**Alternative.** Bind and advertise the bind address when it is a specific
IP (`LEGION_WORKER_HOST=127.0.0.2` advertises `127.0.0.2`).

**Trade-off.** One more rule in a two-input default.

**Why.** Binding a specific address and advertising another is a real
topology (a NAT'd interface), so tying them together would be wrong for
someone. The override already exists for exactly this. **Simplicity of the
default won.**

## 26. `--advertise-host` on the simulator became `--advertise-addr`, defaulting to the route

Made while implementing Phase 1. The simulator's flag is now the same flag
`legion join` and `legion start worker` take, with the same default (the
route's source address toward the manager) instead of `127.0.0.1`. The AWS rig
passes `--advertise-addr`.

**Alternative.** Keep the simulator's flag as it was.

**Trade-off.** Scripts that passed `--advertise-host` break (no alias, per
AGENTS.md).

**Why.** Two flags for the same concept is the kind of drift the plan exists
to remove, and `127.0.0.1` as a default only worked when the simulator shared
a host with its manager. **One concept won.**

## 27. `legion net policy test` judges only the built-in classes

Made while implementing Phase 2. `TestNetPolicy` runs on the manager. It
resolves the name there and judges the classes that are the same on every
node (loopback, unspecified, link-local, cloud metadata). A node's own
addresses and its `hostGuard` entries are judged only on the worker.

**Alternative.** Ask a node to judge (a worker-plane RPC), or carry each worker
type's overrides into the manager's check.

**Trade-off.** `net policy test` can say `allow` for a target a node refuses
because it is that node's address or in its `hostGuard.deny`. The node's
`net.denied` event is the authority, and the docs say so.

**Why.** A dry-run that needs a live node per question is a heavier tool than
the one agents call in a loop. The built-in classes cover the cases people
ask about (metadata, `localhost`). **Simplicity of the dry-run won.**

## 28. A name that does not resolve is a failed dial, not a denial

Made while implementing Phase 2. When an external target's name does not
resolve, the guard has no address to judge. The connection fails like any
dial failure (`502` in HTTP mode, a closed connection at L4) with no
`net.denied` event. A `proxy` handler may still take the flow, since the
handler resolves the name itself.

**Alternative.** Record a `host-guard` denial for unresolved names.

**Trade-off.** A typo in an external upstream shows up as a dial error, not a
policy verdict.

**Why.** Reporting a denial for something that was never judged would make
the guard look like it refused a name it never saw an address for. **Accurate
verdicts won.**

## 29. A gated twin is the port's `to`, or the advertised number itself

Made while implementing Phase 3. The plan says every gated allocation gets
"twins" for its declared ports. Twins used to be fresh ports from the node's
dynamic range, because a host-mode task shares the host's ports with the
datapath. A gated task has its own namespace, so its twin is now `to` when
the port declares one, else the advertised port number:

```nickel
network = { ports = [{ name = "pg", to = 5432 }] },   # Postgres listens on 5432 inside
```

The datapath owns host port `20123` and forwards to `5432` inside the
namespace. `LEGION_PORT_PG` is `5432`.

**Alternative.** Keep dynamic twins for gated allocations too.

**Trade-off.** Two ports in one task that declare the same `to` collide inside
the namespace, as they already do on macOS. A gated task that has an ingress
chain sees a different `LEGION_PORT_<NAME>` than before (the advertised
number or `to`, not a dynamic twin).

**Why.** Most images listen on a fixed port. With dynamic twins, every stock
image (nginx on 80, Postgres on 5432) would need its command rewritten to read
`LEGION_PORT_<NAME>` before it could run gated, and gated is now the default.
`to` already means "the port inside the container" on macOS, so this gives it
the same meaning on Linux. **Ease of the default won.**

## 30. The node's `os` fingerprint decides `auto`; a node without it stays `host`

Made while implementing Phase 3. Placement reads the node's `os` fingerprint
attribute. Only `linux` turns a container's `auto` into `gated`. A submit-time
check (no node yet) treats the node as not Linux. A Linux worker that cannot
create namespaces (not root) is not detected at placement. The container
fails to start with `gated network namespace ... is not ready`, and the fix
is `network.mode = "host"` or running the worker as root.

**Alternative.** Have workers report a `network.gated` capability and gate on
it.

**Trade-off.** A misconfigured worker fails at start rather than being
skipped at placement.

**Why.** The container runtime already needs a root worker on Linux (it runs
the OCI runtime and mounts), so a node that can run containers can create
namespaces. A capability attribute would add a second source of truth for the
same fact. **Simplicity won.**

## 31. The `gated-default` warning counts pool-scoped policy objects as coverage

Made while implementing Phase 3. `job run` and `job plan` warn when a task
would run gated with no egress allowed. A stored policy object with an egress
chain counts as coverage when its selector matches, ignoring `nodePools`,
because the pool is not known until placement. `job validate` runs locally,
cannot see stored policies, and warns whenever the task itself allows
nothing.

**Alternative.** Warn unless a policy matches every eligible pool.

**Trade-off.** A job that lands in a pool its policy does not cover gets no
warning, then a `403` with `gated-default` and a hint.

**Why.** A warning that fires on correct jobs teaches people to ignore it. The
`403` itself names the source and the fix. **A quiet, accurate warning won.**

## 32. Join tokens keep the manager's own scheme; workers dial TLS regardless

Made while implementing Phase 4. The plan says a manager that answers TLS on
its API port should put `https://` in its join tokens. The token's address is
also the issuer a worker checks boot tickets against (`iss`), and the manager
mints those tickets with its workload-identity issuer URL, which operators
set to that same address (`http://` unless `server.tls` is on). So the token keeps that address, and the worker treats
it as a host and port: every connection to it is TLS, naming
`manager.legion`.

**Alternative.** Rewrite the token to `https://` and the ticket issuer with
it.

**Trade-off.** A token can read `http://10.0.0.5:4747` while every byte the
worker sends to that address is encrypted, which can surprise someone reading
it.

**Why.** Changing the scheme in two places that must agree, for a field the
worker no longer uses to pick a transport, adds a failure mode and buys
nothing. The docs say the worker always uses TLS. **Simplicity won.**

## 33. The manager picks its certificate by the name the client asks for

Made while implementing Phase 4. With `server.tls` on, the manager holds two
certificates on one port: the operator's, for the CLI and browsers, and the
cluster server certificate, for workers. The client's TLS server name picks
one: `manager.legion` gets the cluster certificate, anything else (including
no name, which is what a client dialing an IP address sends) gets the
operator's. Workers always send `manager.legion`, including for the JWKS
fetch, which is why that fetch uses its own small HTTP/1.1 client instead of
`reqwest` (whose server name comes from the URL).

**Alternative.** A second port for workers.

**Trade-off.** A custom JWKS client to maintain, and an operator who points
the CLI at `manager.legion` by name gets a certificate their CA doesn't
know.

**Why.** One port is one firewall rule and one advertised address, which is
what the plan promised operators. **Ease of operation won.**

## 34. A bundle accepted under a token outlives the CA that token pins

Found while implementing Phase 4. Phase 1 checked a persisted bundle, and every
re-issued one, against the join token's pin. After a CA rotation's overlap
ends, the bundle no longer holds the pinned CA, so a worker that restarted
then refused its own cluster. Now a worker records which token pin its bundle
was first accepted under (`<identity dir>/token-pin`). The same token keeps
trusting that bundle, and the manager must chain to the bundle, not the pin,
from then on. A new token (a re-join, another cluster) must still find its
pin in the bundle.

**Alternative.** Require a re-join after every rotation, or keep the old CA in
bundles for good.

**Trade-off.** A worker whose state directory holds a bundle from this token
trusts that bundle without re-checking the pin, so tampering with that
directory changes what it trusts. The directory already holds the node's
private key.

**Why.** A rotation that silently strands every worker that restarts later is
a trap, and the plan promised rotation breaks nothing. The bundle arrived over
a connection verified against the pin or an earlier bundle, so trust chains
back to the token. The plaintext JWKS fallback is gone too: a worker builds a
ticket verifier only over this channel, so "refuse a plaintext JWKS URL" holds
by construction. **Rotation that breaks nothing won.**

## 35. Each allocation gets its own chain, so a drop names its allocation

Made while implementing Phase 5. The plan's sketch puts every allocation's
drops in one shared chain, with a pair of per-allocation accept rules ahead of
them. Counters there can't say whose socket was dropped. The table instead
gives each allocation its own chain (its own VIP block and upstream ports
accepted, then the floor, each class with its own counter and a comment
naming the class), and an `unattributed` chain for sockets in the cgroup
parent that no listed leaf owns.

```text
net.denied  alloc 88213  egress  cloud metadata  source=host-guard  "dropped in the kernel: cloud metadata; ..."
```

**Alternative.** One shared floor chain, with drops reported per node.

**Trade-off.** The table grows by about ten rules per allocation, and every
change to the set of allocations reloads it (which also resets counters; the
worker resets its baseline with it).

**Why.** "A drop happened somewhere on this node" sends an operator hunting;
the plan's point is that `net doctor` names the allocation and the cause. A
few hundred rules is nothing to the kernel. **Diagnosability won.**

## 36. Enforcement off still means a cgroup leaf where the kernel guard runs

Made while implementing Phase 5. The guard matches sockets by cgroup leaf, so
with `resourceEnforcement.enabled = false` the worker's governor still hands
every task `legion/share/legion-<alloc>`: it creates the directory and writes
nothing into it (no controllers, no cpuset, no limits). `cores` are not
reserved in this mode.

**Alternative.** Match by the allocation's processes some other way (UID per
allocation, or a socket mark set by the runtime), or skip the guard when
enforcement is off.

**Trade-off.** One more mode in the governor, and a node with enforcement off
now has a `legion/` tree.

**Why.** Enforcement is on by default, so this mode is the exception; the
cgroup is the one identity every runtime already carries (runc, the exec
supervisor, the VMM). Skipping the guard would make a scheduling knob silently
turn a security floor off. **One consistent floor won.**

## 37. The seccomp profile is a denylist over Docker's blocked set, not Docker's JSON

Made while implementing Phase 6. The plan says to embed Docker's default
profile verbatim. That profile is an allowlist of roughly 350 syscalls across
architectures, with argument filters and capability-conditional blocks.
Legion ships the inverse: allow by default, refuse with `EPERM` the syscalls
Docker documents as blocked (mounts and the new mount API, `unshare`,
`setns`, namespace flags on `clone`, modules, `kexec`, `bpf`, the keyring,
clocks, `reboot`, and the rest), and `clone3` with `ENOSYS`.

**Alternative.** Vendor Docker's JSON with its Apache-2.0 notice.

**Trade-off.** A syscall a future kernel adds is allowed until the list names
it, where Docker's allowlist refuses it by default. Some calls Docker allows
only with an argument filter (`personality`) are allowed outright.

**Why.** A reproduction of a 900-line allowlist that is wrong for one
architecture breaks containers in ways that look like application bugs, and
there is no way to verify one against every runtime here. The denylist is
short enough to review line by line, covers what an escape needs (namespaces,
mounts, modules, `bpf`, the keyring), and the doc states the difference.
Revisit by vendoring the real profile once a CI runner can run it on every
runtime. **Reviewability won.**

## 38. The task directory is an idmapped mount, not chowned to the range

Made while implementing Phase 6. The plan says to chown `/legion/task` to the
range's root, the way `exec` hands its tree to `user`. The worker rewrites
files there after start (KV files with `onChange = "update"`, templates), and
a file it creates as host root after the chown would be unreadable to a
`0600`-mode consumer inside. So the task directory is an idmapped mount, the
same as volumes: on disk it stays host-owned, inside it is container root's.

**Alternative.** Chown at start, and chown every later write.

**Trade-off.** Idmapped mounts need Linux 5.12+ and a supporting filesystem
(tmpfs from 6.3), so an older node fails a userns container's start instead
of running it.

**Why.** Every write path would otherwise need to know about ranges, and one
missed path is a silent permission failure. The kernel floor is already
required for volumes. **One mechanism won.**

## 39. Inside a user namespace, `/sys` is a read-only bind

Made while implementing Phase 6, settling the plan's open ordering question.
Mounting `sysfs` needs the network namespace to belong to the container's
user namespace, and neither a gated namespace (the worker creates it) nor the
host's does. Legion binds the host's `/sys` read-only (`rbind`, `nosuid`,
`noexec`, `nodev`, `ro`) whenever the container has a user namespace, instead
of creating the gated namespace inside the allocation's user namespace.

**Alternative.** Create the gated namespace inside the user namespace, so
`sysfs` mounts.

**Trade-off.** The container sees the host's `/sys` (read-only), including the
host's network interfaces under `/sys/class/net`, rather than a view of its
own namespace.

**Why.** It works on every runtime (it is what runc falls back to on its own)
and keeps the worker's namespace code unchanged. Verified with runc 1.5, gated
and host, in `container_start_latency_host_vs_gated_benchmark` and the
`oci_runtime_security_*` tests; youki and crun were not available here.
**Portability won.**

## 40. Any runtime plugin but `noop` is host-level

Made while implementing Phase 7. The plan's table classifies `exec`,
`container`, and `sandbox`. Runtimes also load as plugins
(`plugins/tasks/<name>`), and a plugin can run anything on the host. Legion
classifies every runtime it does not know as host-level, except `noop`, which
runs nothing.

```text
permission denied: job "x" has a host-level task ("t" uses runtime "custom", a plugin
whose isolation Legion cannot vouch for). Submitting it needs a job:write rule with host = true
```

**Alternative.** Treat unknown runtimes as sandboxed, or let a plugin declare
its own isolation.

**Trade-off.** A plugin that is in fact sandboxed needs the host grant to
deploy until a plugin can declare that.

**Why.** A security classification that defaults to "safe" for code Legion
has never seen is the wrong default. A plugin-declared isolation flag can
come later without changing this rule for built-ins. **The safe default
won.**

## 41. A `dev` sandbox is host-level on every OS

Made while implementing Phase 7. The plan says `hypervisor = "dev"` is
host-level "on Linux", where it is an OCI bundle. On macOS it is an Apple
`container` VM. Submit happens before placement, so the node's OS is not known
then; any `dev` sandbox needs the host grant.

**Alternative.** Classify by the job's constraints, or re-check at placement.

**Trade-off.** A macOS developer with a scoped token needs `host` for a `dev`
sandbox that would have run in a VM. Single-box flows use the management
token, which has every grant.

**Why.** A check at placement would turn a permission error into a stuck
allocation. Dev mode is for development, where the management token is the
norm. **A check that fails at submit, by name, won.**

## 42. The host grant gates spec submission, not scale or instances

Made while implementing Phase 7. `RunJob` and `RunJobTemplate` check `host`.
`ScaleTask`, `CreateInstance(s)`, `StartInstance`, and `StopJob` do not: they
change how many copies of an already admitted spec run, not what runs.

**Alternative.** Check `host` on every `job:write`.

**Trade-off.** A `job:write` token without `host` can scale up an `exec` job
someone with the grant deployed.

**Why.** The grant is about what code runs where, which only a spec decides.
Gating scale would make every operator who resizes jobs a host administrator,
the same over-broad grant the plan avoided for bands. **The narrow grant
won.**
