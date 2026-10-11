# Judgement Calls: Sandbox Exec

Decisions made while writing
[`plans/sandbox-exec-plan.md`](../plans/sandbox-exec-plan.md), before any
code exists. Entries 2, 3, 5, 6, and 7 started in the sandbox scaling plan's
calls doc and moved here with the exec work. **Entries are ranked by project
impact, highest first. The number is the rank.** Implementation-time calls
get inserted at their rank as the phases execute. If a citation elsewhere
names an entry by number, fix it when you renumber.

## 1. Direct exec is a third caller class on the worker-plane-auth listener, not a scheme of its own

**Call.** Worker authentication belongs to
[worker-plane-auth-plan.md](../plans/worker-plane-auth-plan.md) (PR #16):
an mTLS listener, manager-only RPCs checked by a `legion://manager/<id>`
URI SAN, and `StartClaim` authorized by its boot ticket. Manager-proxied
exec uses that plan's manager certificate unchanged. Direct exec adds one
caller class to that plan's table, `ExecTask` with an exec ticket and no
manager certificate, verified offline the way `StartClaim` is (plan §9).

```console
$ grpcurl -cacert ca.pem 10.0.3.17:4848 legion.Worker/ExecTask
ERROR:
  Code: Unauthenticated
  Message: ExecTask needs a Legion manager certificate or an exec ticket
```

**Alternatives.**
- (a) The first draft of this work, which designed its own scheme: every
  `ExecTask` carries a ticket, the manager proxy mints one for itself, and
  the other worker RPCs carry a manager-signed bearer token.
- (b) A separate direct-exec listener on its own port with its own TLS.

**Trade-off.** Direct exec cannot ship before worker-plane-auth Phase 1,
and it needs three amendments to a plan in another PR (plan §9). The two
plans have to be implemented in the right order, or together.

**Why (simplicity and security).** Two authentication schemes on one
listener is the kind of seam an attacker finds first. (a) would have
duplicated what PR #16 does better: certificates carry roles, the CA hash
is pinned in the join token, and the manager verifies the worker it dials.
(b) is a second port to open and a second PKI to rotate. One listener with
caller classes keeps one rule per RPC, which that plan already tests by
parsing the proto.

## 2. Every microVM gets a vsock device and the guest agent, after the gated plan's restore override

**Call.** Exec into Linux microVMs (Phase 1) gives every microVM a vsock device
and the agent, not only gated ones. For `tap` and `none` sandboxes the
device carries host-initiated connections only, because the worker binds
no guest-facing listeners. Phase 1 lands after gated plan Phase 1.

```console
$ legion exec --instance user-42 -it -- /bin/bash
root@user-42:/workspace#
```

**Alternatives.**
- (a) Exec only for gated sandboxes, which already have the device.
- (b) A serial-console or SSH-in-guest path that needs no vsock.
- (c) Ship Phase 1 now, before the restore override.

**Trade-off.** This amends the gated plan's definition of `none` ("no vsock
device, no agent"). A `none` sandbox now runs an agent with a host-reachable
listener. The guest still cannot dial out, but the agent is code in every
guest. Phase 1 waits on two gated phases.

**Why (simplicity and security).** (a) makes exec depend on the network
mode, which is the kind of hidden coupling that turns into a support
ticket. (b) needs a network inside the guest and its own keys, and a
serial console has no pty resize or clean exit codes. (c) breaks memory
checkpoints for TAP and `none` sandboxes, which work today, because the
vsock path is baked into the VMM's saved state until the override exists.
Kata and E2B both put an agent in every guest. The air gap that matters,
guest-initiated traffic, is unchanged.

## 3. Direct exec is opt-in, the manager proxy stays the default, and a gateway comes later

**Call.** Exec without the manager in the data path ships as worker-verified
tickets on the worker-plane-auth listener (Phase 2), off by default. The
CLI uses it with `--direct`, and platforms mint tickets with `MintExecTicket` or a
delegated key. The stateless gateway (Phase 4) is built only when browser
or public access at scale needs it.

```console
$ legion exec --instance user-42 --direct -it -- /bin/bash
```

**Alternatives.**
- (a) Gateway first: keep worker addresses private and scale exec with a
  stateless tier from day one (Cloudflare's shape).
- (b) Make direct the default for every client once Phase 2 lands.
- (c) Redirects: the manager answers `ExecAllocation` with a redirect the
  client or the manager follows, as Kubernetes once did.

**Trade-off.** Direct exec reverses the documented "clients never learn
worker addresses" rule for callers who opt in, and it needs a network
route from client to worker and trust in the cluster CA. Browsers keep
going through the manager until Phase 4, so the UI does not scale past the proxy yet.

**Why (simplicity, then performance).** The common agent-platform caller is
the platform's own backend, already inside the network, which can reach
workers privately. (a) adds a tier every operator has to run before anyone
benefits. (b) breaks every human user whose laptop cannot route to
workers. (c) is what Kubernetes removed after CVE-2018-1002102, because a
followed redirect authorizes nothing. Modal ships the same token-plus-
worker-address design for its own exec, which is the best evidence it
holds up at scale.

## 4. MicroVM exec ships only on an authenticated worker plane

**Call.** Phase 1 (exec into microVMs) lands after worker-plane-auth Phase
1, not before it, even though the vsock work does not need it to function.

**Alternative.** Ship Phase 1 as soon as the gated plan's restore override
lands, on today's listener, and document the local gap.

**Trade-off.** Microvm exec waits on a second plan. If PR #16 slips, so
does a shell into sandboxes.

**Why (security).** Today the worker binds `127.0.0.1:4848` with no
authentication (`src/worker/daemon.rs:155`, `src/worker/server.rs:200`), so
any process on the worker's host can call `ExecTask` against any
allocation on the node, including an `exec` task or a host-network
container. Sandboxes are exempt only because they cannot be exec'd into.
Phase 1 removes that exemption, so shipping it on the open listener would
let a local workload open a root shell in every sandbox on its node,
which is the isolation sandboxes exist to provide.

## 5. Replay and revocation for direct exec use local state and TTLs, not raft

**Call.** A ticket names one node. Single use is the node's local spent-`jti`
set, bounded by expiry, the same structure boot tickets use. A restarted
worker refuses tickets issued before it started. Revocation is the TTL (60
s to redeem by default, 10 min ceiling), plus revoked accessor ids pushed to
workers in the heartbeat reply, which end open direct sessions. A session
is capped by `max_session` (1 h by default).

**Alternatives.**
- (a) Record spent tickets in raft, for global single use and a revocation
  list.
- (b) Have the worker call the manager to validate each ticket.
- (c) Re-check the caller's credential every 30 s, as the proxy does.

**Trade-off.** Revocation of an open direct session lags by up to one
heartbeat interval, which adaptive cadence can stretch to about 21 s on a
very large fleet. A client whose ticket was minted just before a worker
restart has to mint again. A ticket stolen in its redeem window opens one
session.

**Why (scale).** (a) is a raft write per session, the cost the
[scaling plan](../plans/sandbox-scaling-plan.md) removes everywhere else. (b) puts the manager back on the path of every
session open. (c) needs the user's credential on the worker, which a
ticket exists to avoid. Binding to one node makes a local cache strict
single use, which RFC 9449 notes is otherwise infeasible without shared
state.

## 6. Platforms mint exec tickets with delegated keys scoped to job globs

**Call.** Phase 3 lets a platform register a public key with a job glob and a
TTL ceiling (`CreateExecKey`, one control-lane write per key). Workers
trust tickets signed by it for matching jobs on non-host runtimes.

**Alternative.** Platforms always call `MintExecTicket`, so the manager
signs every ticket.

**Trade-off.** A leaked platform key is a shell capability for every
matching job until the key is deleted, and deletion reaches workers on
their next heartbeat. Operators manage one more kind of secret.

**Why (performance).** With session mode and edge boot, a platform can
start a sandbox with no manager call. Requiring a manager call to open a
shell into it would bring the manager back onto the hottest per-session
path. The glob and the `host` exclusion keep a key narrower than the
`exec:write` grant that created it.

## 7. Exec over vsock reuses the existing exec frames, behind a version hello

**Call.** The worker and the guest agent speak the same length-delimited
`ExecClientFrame` and `ExecServerFrame` protobufs every other exec leg
uses, after a one-message version hello.

**Alternatives.** Kata's ttRPC agent API (`ExecProcess`, `WriteStdin`,
`ReadStdout`, polled per stream), or E2B's connect-rpc process service.

**Trade-off.** Legion's frames have no reattach or output offsets, so a
dropped vsock connection ends the session, as a dropped manager connection
does today.

**Why (simplicity).** One frame format end to end means the worker splices
bytes instead of translating, and the agent has one codec. The gated plan
requires any agent protocol beyond raw bytes to negotiate, because a
restored guest can run an older agent than its worker, and the hello is
that negotiation.
