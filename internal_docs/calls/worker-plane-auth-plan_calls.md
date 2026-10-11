# Judgement Calls: Authenticated, Reachable Worker Plane

Plan: [worker-plane-auth-plan.md](../plans/worker-plane-auth-plan.md)

Entries are ranked by project impact, highest first. An entry's number is its
rank. These were made while writing the plan, before any code, so each one is
also an open invitation to argue in review.

## 1. Managers dial workers over mTLS, not a worker-initiated reverse channel

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

## 2. The worker binds all interfaces by default

Once every privileged RPC requires a manager certificate, the worker binds
`0.0.0.0:4848` with no flag.

**Alternative.** Keep the loopback default and require
`LEGION_WORKER_HOST=0.0.0.0` for multi-host. Secure-by-default in the narrow
sense: nothing listens on the network unless asked.

**Trade-off.** A TLS listener is exposed on every worker by default. Its attack
surface is the TLS stack and `StartClaim`'s ticket verification.

**Why.** The opt-in path is the bug we are fixing. `legion join` on a second
box would still silently break exec, logs, and the mesh until someone found
the env var. The listener is safe to expose because of 6.1, not because of the
bind, and a default that only works on one machine contradicts "adding a
worker is two commands." **Ease won.**

## 3. One cluster CA, with roles in URI SANs

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

## 4. The join token pins the CA hash

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
error: the manager at http://10.0.0.5:4747 presented cluster CA sha256:51aa07...,
but this join token pins sha256:9c1e4f...
```

## 5. Workers send a CSR, departing from mesh_ca's manager-generated keys

The worker generates its key and sends a CSR. The manager no longer returns
private keys to new workers.

**Alternative.** Keep the current design (`src/manager/mesh_ca.rs:72-75`):
the manager generates both halves, which its own comment justifies as "a CSR
flow is more ceremony than a 24-hour credential warrants."

**Trade-off.** More code on both sides, a persisted key on the worker, and two
issuance paths during the compatibility window.

**Why.** That comment was right when the cert guarded only mesh flows. Now it
guards a node's identity to the control plane, and the channel it travels over
stays plaintext until Phase 3. A key that never leaves the node removes a
whole class of on-path attacks for the cost of one optional proto field.

## 6. `StartClaim` shares the listener, with optional client certs

One TLS listener. Manager-only RPCs check for a manager certificate, and
`StartClaim` keeps ticket-only authorization.

**Alternative.** A second port for edge-boot routers, with mandatory client
certs on the manager port.

**Trade-off.** The TLS handshake succeeds for anyone. Authorization happens
per RPC, so a missing check on a new RPC would be a hole. The classification
test exists to catch that.

**Why.** One port per worker keeps firewall rules and docs simple, and routers
benefit from TLS too, since they now know they reached the real node. The
per-RPC check mirrors how the manager already enforces ACLs, including the
"fail the build if unclassified" guard.

## 7. The advertise address is auto-detected from the route to the manager

The worker advertises the local address it uses to reach its manager, unless
`LEGION_WORKER_ADVERTISE_ADDR` or `--advertise-addr` says otherwise.

**Alternative.** Require the address explicitly, or advertise the first
non-loopback interface.

**Trade-off.** Behind NAT, or with asymmetric routing, the guess is wrong and
only the Phase 2 probe tells you.

**Why.** On the common topology (managers and workers on one private network)
the source address toward the manager is exactly the address the manager
should dial back, and a co-located worker still gets `127.0.0.1`. "First
non-loopback interface" picks Docker bridges and VPN tunnels. Requiring a flag
breaks the two-command join.

## 8. Plaintext stays allowed only to loopback, for one release

A new manager talks plaintext only to a protocol-2 worker advertising a
loopback address, and drops that path one release after Phase 1.

**Alternative.** Keep plaintext for any old worker, or remove it immediately.

**Trade-off.** One more release of a branch in the client factory.

**Why.** Old workers only ever advertised `127.0.0.1`, so plaintext to loopback
is the entire set of setups that worked before. Keeping it makes the rolling
upgrade invisible. Allowing it to remote hosts would preserve nothing that
worked and would keep the hole open.
