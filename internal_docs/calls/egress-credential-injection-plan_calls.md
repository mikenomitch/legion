# Judgement Calls: Egress Credential Injection

Calls made while building
[egress-credential-injection-plan.md](../plans/egress-credential-injection-plan.md).
**Ordered by project impact, highest first; an entry's number is its rank.**
Public reference: [docs/network-handlers.md, "Credential injection"](../../docs/network-handlers.md#credential-injection).

## 1. Inject only on external `https://` upstreams; never intercept TLS

**Call.** Credentials are declared on a `connect.upstreams` entry whose `to`
is `https://`, where the task already speaks plaintext HTTP to a loopback
port and the node originates TLS. Submit refuses every other placement:

```text
task agent connect.upstreams entry 'openai' credentials: to = 'http://api.openai.com' is
plaintext, so the injected secret would cross the network in the clear. Hint: use https://
(the node originates TLS; the task still speaks plain HTTP to its loopback port)
```

`tcp://`/`tls://` ("no request head to put a header in") and catalog-service
upstreams are refused too. Nothing is injected into `CONNECT` tunnels through
the gated egress proxy.

**Alternatives.** (a) agent-substrate's design: terminate the workload's
HTTPS with a Legion CA trusted inside the sandbox and swap headers on any
destination, so unmodified clients work with no base-URL change. (b) Allow
`http://` upstreams too, for internal plaintext APIs.

**Trade-off.** Clients must be pointed at the upstream
(`OPENAI_BASE_URL=http://$LEGION_UPSTREAM_OPENAI/v1`); a tool that hardcodes
its HTTPS endpoint cannot use injection. Internal plaintext origins cannot
receive an injected credential.

**Why (simplicity, and the security boundary).** The public docs promise
Legion will never intercept TLS, and a CA inside every sandbox that can
impersonate the internet is a far larger trust grant than the problem
needs. External upstreams already give the node the request head without
interception, so injection is one header on a request the node builds
anyway: no new datapath, no trust-store management. `http://` would put the
secret on the wire in clear, which is the opposite of the feature's point.

## 2. Fail closed: a missing credential answers `503` locally, without dialing

**Call.** If the node holds no value for a declared header (never fetched,
or revoked), the request is answered by the forwarder:

```http
HTTP/1.1 503 Service Unavailable
Legion-Denied-By: credential-unavailable
Legion-Denied-Reason: credential-unavailable: upstream openai header authorization: kv secrets/openai not found
```

**Alternatives.** (a) Send the request without the header and let the
origin answer `401`. (b) Send the task's own value through (the
placeholder).

**Trade-off.** A worker restart, or a manager outage at an allocation's
first heartbeat, makes requests on that upstream fail until the first
successful fetch, even if the origin would have accepted an anonymous
request.

**Why (simplicity, and correctness).** An unauthenticated request can
succeed against an origin with an anonymous tier and silently do the wrong
thing, and a burst of `401`s looks like a provider problem rather than a
Legion configuration problem. A local `503` that names the key tells the
agent (and the responder) exactly what is wrong and costs no origin quota.
Mirrors the gated datapath's fail-closed stance on pending policy.

## 3. Naming a credential path needs `kv:read` on it at submit

**Call.** RunJob and template runs require `kv:read` on every
`credentials[].from.kv` path, with
`(connect.upstreams[openai].credentials names kv secrets/openai; the submitter needs kv read on it)`.

**Alternatives.** (a) Only `job:write`, trusting that the value never
reaches the submitter. (b) A dedicated `kv:inject` action, so a team can use
a key without being able to read it.

**Trade-off.** A team that should be able to *use* a key but not *read* it
cannot be expressed; they need read. (b) would express it.

**Why (simplicity).** Without the check, `job:write` would let anyone send
any KV secret to a host of their choosing (`to = "https://attacker.example"`),
which is read access with extra steps. (b) is honest only if `to` is also
constrained, which needs a policy model we do not have; same rule as
`certFrom = { kv }`, so one sentence explains both. Revisit (b) with
destination-scoped KV grants.

## 4. Overwrite the task's header; no placeholder matching

**Call.** For each declared header, every value the task sent (any case,
any count) is dropped and the header is set to `prefix` + the secret. A
placeholder is unnecessary but harmless, so SDKs that insist on
`OPENAI_API_KEY` work with `placeholder`.

**Alternatives.** (a) agent-substrate's swap: replace only a known
placeholder value, leave other values alone. (b) Envoy's `overwrite = false`:
add only when the task sent none.

**Trade-off.** A task cannot deliberately send its own value for that
header to that upstream (for example, a per-user key it obtained itself).
It would need a second upstream without credentials.

**Why (simplicity, security).** Placeholder matching adds a second
declaration (`placeholder = "..."`) and a failure mode (the SDK formats the
placeholder slightly differently and the real key is never added). "Keep the
task's value if present" lets a compromised task override the credential the
operator declared. Overwrite is one rule with no configuration.

## 5. Delivery is a per-heartbeat pull, like `certFrom = { kv }`

**Call.** The worker calls worker-plane `GetUpstreamCredentials` once per
heartbeat, only while a desired allocation declares credentials, sending
the versions it holds; unchanged values cost no bytes. Triggered from the
heartbeat loop after the desired set (reply or `StreamWork` mirror) is
applied.

**Alternatives.** Push values in the heartbeat reply (or `StreamWork`)
alongside the desired allocations.

**Trade-off.** Rotation reaches a node after up to one heartbeat interval
plus one extra RPC per beat on nodes that declare credentials.

**Why (performance, simplicity).** The heartbeat reply is a hot, cached,
streamed payload that is also written to `netd_state.json`; secrets in it
would need redaction at every one of those places and would ride every
reply to every node. A separate RPC keeps secrets off the hot path and out
of every persisted structure, and reuses the exact shape and ACL reasoning
of the certificate RPC (one pattern, not two).

## 6. The identity provider names a task identity, which may then skip delivery

```nickel
identities = [{ name = "api", audience = ["https://api.internal.example"], ttl = "15m" }],
connect.upstreams = [{ name = "api", to = "https://api.internal.example",
  credentials = [{ header = "Authorization", prefix = "Bearer ", from = { identity = "api" } }] }],
```

**Call.** `from = { identity = "<name>" }` refers to an entry in the task's
`identities`, which owns the audience, TTL, `sub` template, and extra
claims. An identity named this way may set neither `env` nor `file`; the
worker then skips it entirely (`parse_identities` drops undelivered
identities), so the token exists only in the manager's reply and the
forwarder's memory. Such an identity may not set `changeMode` or
`changeSignal`. This deviates from the plan's sketch, which had the
audience inline: `from = { identity = { audience = "..." } }`.

**Alternatives.** (a) The plan's inline form, with the manager synthesizing
an identity from the credential. (b) Require the identity to be delivered
too, so the existing rule ("set env or file") never bends.

**Trade-off.** Two blocks to write instead of one, and a validation rule
that now depends on another block (`TaskConfig::validate_identities` reads
`connect`).

**Why (simplicity, extensibility).** The identity block is where the
security-relevant choices about a token already live, and the rules that
guard them (the `sub` uniqueness floor, reserved claims, TTL parsing) apply
unchanged. An inline form would either grow a second copy of those fields
or silently use cluster defaults for `sub` and TTL. (b) would put the token
back into the task, which is exactly what injection exists to avoid.

## 7. The manager mints and decides when to refresh; the token's expiry is its version

**Call.** `GetUpstreamCredentials` mints the token itself, the way
`IssueWorkloadIdentityToken` does. The worker sends the token's expiry as
`known.version`; the manager answers `unchanged` while more than half the
identity's TTL remains, and mints a new token otherwise. The worker side is
the KV path unchanged: same RPC, same in-memory map, same fail-static rules.

**Alternatives.** The plan's "minted on the node": the worker calls
`IssueWorkloadIdentityToken` and runs a refresh loop per injected identity,
as it does for delivered ones.

**Trade-off.** A token's refresh is quantized to the heartbeat interval, and
an identity whose TTL is under two heartbeats would be re-minted on every
beat. The node also holds a token for as long as the manager is
unreachable, until the origin rejects it as expired.

**Why (simplicity).** One delivery path for every provider: the forwarder
never learns what kind of credential it holds, and the refresh policy sits
next to the signing key that sets the expiry. A second refresh loop on the
worker would duplicate the delivered-identity machinery for a value that
never touches the task.

## 8. Never inject the control-plane identity

**Call.** `from = { identity = "legion" }` is refused at submit: `... is the
control-plane identity, which authenticates to Legion itself; injecting it
would hand a Legion credential to https://...`.

**Alternatives.** Allow it, for an upstream that is the cluster's own API.

**Trade-off.** A task cannot use injection to call Legion's own API with its
control-plane token; it reads the delivered token instead.

**Why (security).** Every injection target is an external origin, so the
only effect of allowing it would be to hand a token that Legion accepts to
a third party.

## 9. Deleting the key revokes; other errors keep the last good value

**Call.** The reply marks authoritative problems `absent` (the key does not
exist, or the allocation no longer declares the header); the worker drops
the value and the upstream fails closed. A KV read error, a value that
cannot be a header, or a failed call keeps the last good value.

**Alternatives.** Follow `certFrom` exactly: every error is fail-static and
a deleted key keeps serving the previous material.

**Trade-off.** One more proto field (`absent`) and a deliberate divergence
from the certificate behavior; a mistaken `kv delete` takes the upstream
down at the next heartbeat.

**Why (security).** For a TLS certificate, serving the old one is safe and
keeps a site up. For an API credential, deleting the key is the obvious
revocation gesture, and an operator who did it expects requests to stop.
Transient errors still fail static, so a manager blip never takes an
upstream down.

## 10. Inject into `proxy` handler hand-offs, and say the proxy sees it

**Call.** A request handed to a `proxy` hop carries the injected header,
read once per request and also sent on every retry. The docs say a proxy hop
sees the credential.

**Alternatives.** Strip credentials on a hand-off, so the operator's proxy
never sees the secret.

**Trade-off.** Any proxy in the path holds the key in its memory for the
request, and the hand-off is plain HTTP in absolute form (the proxy opens
TLS to the origin), so a `service =` proxy on another node receives the key
unencrypted over the network. Documented, with the advice to use a
`node`- or `port`-local proxy. Sending the hand-off over the mesh would close
that and is worth doing for every hand-off, not only credentials.

**Why (simplicity).** A `proxy` hop answers for the origin; without the
credential its request to the origin fails, which would make credentials and
proxies mutually exclusive. The proxy is operator-declared code already
trusted with the request body.

## 11. Sandbox guests were left out of Phase 1; gated sandboxes are now covered

**Call.** Phase 1 shipped for `exec` and `container` tasks, because no
`sandbox` guest could reach its `connect.upstreams` (the listeners are on
the node's loopback, or in a namespace a sandbox lacks; a gated guest's only
ways out were the vsock proxy and resolver). The same PR then added
upstreams to gated sandboxes, under the gated-sandbox plan (its calls #12
and #14): the guest agent relays each upstream's loopback port and VIP over
vsock to the node's own upstream listener. Injection rides that listener, so
it applies to gated sandboxes with no credential-specific code. TAP-wired and
loopback-only sandboxes still cannot reach their upstreams, documented in
the public limits and TODO.md.

**Alternatives.** (a) Build the relays inside the credential feature. (b)
Ship credentials only for `exec` and `container`, and leave sandboxes to a
later change.

**Trade-off.** TAP-wired sandboxes, which have a NIC and could reach the
origin directly, cannot use injection yet. Nothing about the gated path has
run on a real VMM (G5.2 is open).

**Why (scope, simplicity).** Upstream reachability for sandboxes is a
datapath feature (the guest agent, listener binding, gated readiness, the
restore path), not a credential feature, so it lives in the gated plan and
every upstream feature (HTTP mode, policy, handlers, credentials) gets it at
once. Gated sandboxes are the ones agent platforms run untrusted code in, so
they are where keeping keys out of checkpoints matters first.

## 12. `authz` hops run before injection and never see the value

**Call.** External `authz` webhooks receive the task's own headers (minus
`authorization`, `cookie`, `proxy-authorization`, as before); injection
happens after them, in the request the node sends upstream.

**Alternatives.** Inject first so policy and `authz` decide on the final
request.

**Trade-off.** A webhook cannot make decisions based on which credential is
used (it can still see the upstream and destination).

**Why (security).** Sending the secret to every `authz` webhook would widen
who holds it for no decision value, and contradicts the existing rule that
`authz` never sees authorization headers.

## 13. One trailing line ending is trimmed from the KV value

**Call.** `sk-...\n` injects `sk-...`. A value with an inner CR, LF, or NUL
(or empty after trimming) is refused with an error and not sent.

**Alternatives.** Send the value verbatim and let header construction fail
per request.

**Trade-off.** A secret that genuinely ends in a newline cannot be
injected (none do).

**Why (ease).** `legion kv put secrets/x "$(cat key.txt)"` already strips,
but values written by other tools often end in a newline, and a header
value cannot carry one. Checking on the manager keeps a bad value out of
every node and reports it once, by key.
