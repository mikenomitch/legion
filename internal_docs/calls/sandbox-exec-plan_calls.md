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

## 6. Revocation re-checks open direct sessions on the heartbeat, instead of pushing revoked accessors

*Implementation (Phase 2), deviating from entry 5 and plan §6.4.*

**Call.** Every heartbeat, a worker reports its open direct sessions
(`HeartbeatRequest.exec_sessions`: the ticket's `jti`, its minting `sub`,
the job, the action) and the manager answers with the ones that no longer
hold (`HeartbeatReply.revoked_exec_sessions`, each with a reason). The
manager looks the minting token up by accessor and re-runs the session's
grant against today's policies, the check the proxied path runs every 30
seconds.

```console
$ legion exec 1 --direct -- sh -c 'exec sleep 300'
Error: exec: session ended: the token that minted this session was deleted
```

**Alternatives.**
- (a) The plan's shape: the manager pushes `revoked_accessors` to every
  worker after a token is deleted.
- (b) Re-check nothing; rely on the TTL and `max_session`.

**Trade-off.** One store read per distinct accessor per heartbeat on a node
with open direct sessions, and the worker sends a few bytes per open session
on every beat. A credential with no accessor (a JWT auth method) cannot be
looked up, so its sessions are bounded by `max_session`, which the mint
clamps to that credential's expiry (entry 12).

**Why (simplicity and correctness).** (a) needs every manager to know which
tokens were deleted recently and which workers to tell, which is state that
does not exist in a multi-manager cluster: a token deleted through one
manager would reach only workers heartbeating to it, unless the list were
replicated. It also misses expiry and a policy edit that removes the grant.
Asking "does this still hold?" is stateless, works on any manager, and
covers deletion, expiry, and policy changes with one check. The session ends
with `PermissionDenied` and the reason, as a proxied session does.

## 7. Platforms mint exec tickets with delegated keys scoped to job globs

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

## 8. Exec over vsock reuses the existing exec frames, behind a version hello

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

## 9. `--direct` fails by name when the worker refuses tickets; it does not fall back to the proxy

*Implementation (Phase 2), deviating from plan §12.*

**Call.** `legion exec --direct` against a worker without
`exec = { direct = true }`, or one the client cannot reach, fails with the
reason and a hint to drop `--direct`. It never retries through the manager.

```text
Error: exec: this worker does not admit exec tickets
Hint: set exec = { direct = true } in its worker config, or exec through the manager (drop --direct)
```

**Alternative.** The plan's fallback: retry the session through
`ExecAllocation` with a note.

**Trade-off.** A script that asks for `--direct` on a mixed fleet fails on
the nodes that have not opted in.

**Why (simplicity).** The plan's fallback existed for version skew with old
workers, and Legion has no installed base to protect (AGENTS.md). A silent
fallback would also hide the thing `--direct` exists to guarantee: that the
session's bytes do not cross a manager. The failure names the setting.

## 10. Exec tickets share the boot-ticket JWKS cache, and verification runs off the async runtime

*Implementation (Phase 2).*

**Call.** A worker builds one JWKS cache (`BootTicketVerifier`) when it
accepts boot tickets or direct exec, and the exec verifier decodes through
it with its own audience, node check, process-start check, and spent set.
Both `ExecTask` and `StartClaim` verify inside `spawn_blocking`, and the
pinned JWKS fetch runs on a thread of its own.

**Alternatives.** A second cache for exec tickets; verifying inline in the
handler, as `StartClaim` did.

**Trade-off.** A spawn and a thread hop per ticket presented, and one more
thread per JWKS refresh.

**Why (correctness).** Inline verification blocked a runtime worker thread
on a refresh, and the pinned fetch built a runtime inside the server's
runtime, which panics. With `--run-worker` the manager the fetch dials runs
on the same runtime, so a blocked worker thread could also starve the very
connection it waited on. Edge boot had the same latent bug: its tests used a
static JWKS and never fetched. One cache keeps one refresh schedule per
worker, which plan §9 amendment 3 asked for.

## 11. The worker's direct-exec switch is a top-level `exec` block in the worker type

*Implementation (Phase 2).*

**Call.** `exec = { direct = true }` sits beside `hostGuard` in the worker
type and rides to the worker in the same security payload
(`WorkerConfig.security`).

**Alternatives.** A field under `edgeBoot` (it already holds the JWKS
settings); a worker CLI flag or environment variable.

**Trade-off.** The JWKS refresh and clock-skew settings still live under
`edgeBoot`, so an operator tuning them for direct exec edits a block named
for another feature.

**Why (simplicity).** The plan named the surface `exec = { direct = false }`.
Direct exec widens who can reach the worker plane, which is the security
payload's job, and a per-type setting (not a per-process flag) is what
operators audit. Moving the JWKS knobs is a rename for when someone needs to
tune them.

## 12. A direct session never outlives the credential that minted it

*Implementation (Phase 2).*

**Call.** The mint clamps `max_session` to the minting credential's
remaining lifetime, and refuses an expired one.

**Alternative.** Only `max_session` (1h by default), with the heartbeat
re-check catching expiry.

**Trade-off.** A short-lived token mints short sessions.

**Why (correctness).** The re-check (entry 6) finds credentials by accessor,
and a JWT from an auth method has none. Clamping at mint gives every
credential the guarantee the proxied path gives: the session ends when the
credential does.

## 13. `exec-ticket mint` is cataloged and exposed over MCP as mutating

*Implementation (Phase 2), deviating from plan §8.*

**Call.** `exec-ticket.mint` and `legion_exec_ticket_mint` set
`mutates: true`, so the MCP tool is listed only with `--allow-write`.

**Alternative.** The plan's `mutates: false, read_only_safe: false`.

**Trade-off.** The catalog says "mutates" for a call that writes nothing.

**Why (security).** The plan asked for the tool to be "gated like
`legion_exec_run`", and MCP's only gate is `mutates`. A ticket opens a
shell; an agent in read-only mode must not be able to get one.

## 14. The mint reply carries the cluster CA, and `exec-ticket mint` takes its argv after `--`

*Implementation (Phase 2).*

**Call.** `MintExecTicketReply.ca_pem` is the cluster CA bundle, so a client
can verify the worker from the reply alone. `legion exec-ticket mint` takes
the ticket's command after `--`, as `legion exec` does, not as `--argv`.

```console
$ legion exec-ticket mint --instance user-42 --tty -j -- /bin/bash
```

**Alternatives.** A separate `legion net ca show` call; the plan's `--argv`
flag.

**Trade-off.** Every reply carries a few KiB of PEM. `-j` must come before
`--`, or it becomes part of the argv.

**Why (simplicity).** A platform verifying a worker should not need a second
call (and a second grant) to learn what to trust. `--` is how every exec
command in Legion takes argv.

## 15. A direct action session reports its action name, never its rendered argv

*Implementation (Phase 2).*

**Call.** `ExecAuditRecord.command` is empty for an action session, and the
manager logs `action:<name>`, the subject the proxied path logs.

**Alternative.** Report the argv for every session.

**Trade-off.** The audit for a direct action run cannot show the parameter
values that were used.

**Why (security).** An action's argv is rendered with its parameters, and a
`sensitive` parameter is redacted on the proxied path. The worker cannot tell
which words of the argv came from sensitive parameters, so it reports none.
