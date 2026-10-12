# Judgement Calls: Manager Reverse Proxy

Decisions made while writing
[`plans/manager-reverse-proxy-plan.md`](../plans/manager-reverse-proxy-plan.md),
before any code exists. **Entries are ranked by project impact, highest
first. The number is the rank.** Implementation-time calls get inserted at
their rank as the phases execute. If a citation elsewhere names an entry by
number, fix it when you renumber.

## 1. Browsers authenticate with a one-time code exchanged for a cookie that opens one target

**Call.** `CreateProxySession` returns a URL carrying a signed code that
lasts 60 seconds and is good for one use. The proxy trades the code for an
`HttpOnly` cookie whose `Path` (later, host) is that one target, and
redirects to the URL without the code.

```text
GET /proxy/i/ws-7k2q/http/?legion-code=lpc_3f9…
303 See Other
Location: /proxy/i/ws-7k2q/http/
Set-Cookie: legion-proxy=…; Path=/proxy/i/ws-7k2q/http/; HttpOnly; SameSite=Lax
```

**Alternatives.**
- (a) The bearer token, or a long-lived proxy token, in the query string
  (`?token=`), the way Jupyter does it. It is the simplest option, and it
  works from `curl`.
- (b) A manager login cookie for the whole proxy origin. You log in once and
  every app works, which is the best experience for heavy users.
- (c) The UI sets the cookie with a credentialed cross-origin `fetch`.

**Trade-off.** Every session starts with a click in the UI or a CLI call,
and sessions end after `proxy.sessionTtl` (8h). A bookmark costs one extra
click, and only when `proxy.uiUrl` is set. The code does sit in one history
entry and one access log line, but it is dead by then.

**Why (simplicity and security).** (a) puts a long-lived credential in
browser history, in access logs, and in the `Referer` of every third-party
asset the app loads. (b) needs a login page on the manager origin, and it
gives every proxied page ambient authority over every app. (c) is a
third-party cookie, which browsers block. The code exchange is Coder's
subdomain-app flow. It works unchanged in path mode, in host mode, and
later at the gateway.

## 2. The worker dials the app through a tunnel on the worker plane; the manager never dials node ports

**Call.** A new `Worker.ProxyTunnel` bidi stream carries one TCP connection
per stream, and only the `manager` caller class may open one. The worker
dials `127.0.0.1:<bind_port>` inside the allocation's namespace, and only
for a port that a local, running allocation declares. That is the same last
hop the ingress datapath uses (`src/worker/netd/ingress.rs:521`). HTTP is
spoken at the manager, over the tunnel.

**Alternatives.**
- (a) The manager dials `AllocNetwork.address:host_port` itself. This needs
  no new RPC and no worker change, and it works on one host today.
- (b) An L7 RPC, where the worker runs the HTTP client and returns
  responses.

**Trade-off.** A new worker RPC means version skew: an old worker answers
`Unimplemented`. Every byte pays gRPC framing. The proxy also enters
*behind* the job's ingress chain, so ingress authz handlers and mesh mTLS
do not apply to it (entry 3 covers what that means for grants). Multi-host
waits on worker-plane-auth Phase 1, but (a) waits too: every
`workerAddress` is `127.0.0.1` today (`src/worker/daemon.rs:398`).

**Why (simplicity, extensibility).** (a) needs the dynamic port range open
from managers to every node, sends plaintext across the VPC, and is refused
by mesh-TLS ingress ports. It cannot reach a gated task's private port, and
it turns the manager into a general-purpose dialer. The tunnel needs only
the one worker port that worker-plane-auth already secures. (b) would need
its own WebSocket framing. The L4 tunnel handles upgrades as a splice, and
the same RPC carries the Phase 3 port-forward and, later, the gateway.

## 3. `proxy` is its own ACL resource, not `exec:write` or `job:read`

**Call.** There is a new resource, `proxy`, and `write` opens a session.
It is scoped by job name and namespace, like `exec`.

```nickel
[ { resources = ["proxy"], actions = ["write"], names = ["grafana"] } ]
```

**Alternatives.**
- (a) Reuse `exec:write`. Proxying into Jupyter or code-server *is* code
  execution, so one grant matches the real power, and there is no new
  vocabulary.
- (b) `job:read`, because viewing a dashboard feels like reading.
- (c) A third action, `proxy`, on the `job` resource.

**Trade-off.** It is one more resource to grant. Admins must also know that
`proxy:write` on an app that runs code is as strong as `exec:write` and
skips the ingress chain. The public doc has to say so, and existing
policies grant nothing until someone edits them.

**Why (simplicity for the security admin).** The common request is "let
on-call see Grafana", not "give on-call a shell". The codebase already
splits a resource out when the narrow grant is the common one: `scale`,
`boot`, and `snapshot` all do (`src/structs/acl.rs:14-66`). (b) is far too
broad, because `read` is handed out widely and a proxy can `POST`. (c)
breaks the read/write action model that every policy and the endpoint table
assume.

## 4. Phase 1 routes by a stable per-target subpath, rewrites headers only, and moves to hosts in Phase 2

**Call.** Phase 1 serves `/proxy/{a,i}/<id>/<label>/`. The path names a
target, not a session, so an app can be configured with it through
`LEGION_PROXY_PATH_<LABEL>`. The prefix is stripped. Only `Location` and
`Set-Cookie` `Path` get rewritten, never bodies. Phase 2 adds
`<label>--<id>.<proxy.domain>`.

**Alternatives.**
- (a) Hosts only, from day one. This is Coder's recommendation, and it has
  no shared-origin problem.
- (b) A path per session (`/proxy/s/<session>/…`), so the path itself is
  the capability.
- (c) Rewrite HTML bodies, as the Kubernetes API server proxy does.

**Trade-off.** In Phase 1, every app shares one origin. A malicious
proxied page can request, and read, another app that the same browser
holds a session for. That is a real gap for sandboxes running untrusted
code, so the public doc must state it and point those users at Phase 2.
Apps that cannot take a base path stay broken until Phase 2. Stripping the
prefix also breaks apps like Jupyter that expect to receive it (open
question 1). Rewriting `Set-Cookie` goes beyond the brief, which said
"`Location` only". It is still a header, and without it two apps'
`session` cookies collide.

**Why (simplicity).** Phase 1 needs no DNS and no certificates, which suits
the dev and small-cluster case. (a) makes wildcard DNS and a cert the price
of the first click. (b) makes it impossible to configure an app's base
path. (c) is the brittle part of the Kubernetes design, and SPAs break
under it anyway.

## 5. The proxy gets its own port, is never a trusted origin, and is on by default

**Call.** The proxy listens on `proxy.port` (default `server.port + 3`,
which is 4750), with TLS from `server.tls`. `OriginPolicy` refuses the
proxy's own origins on the gRPC-Web guard and the exec WebSocket, including
loopback ones. `proxy.enabled` defaults to `true`.

**Alternatives.**
- (a) A tower layer on the main listener, as `OidcHttpLayer` does. That is
  one port, with TLS already in place.
- (b) The exec WebSocket listener on `+ 1`, which is already raw hyper with
  upgrades.
- (c) Off by default.

**Trade-off.** Managers get a fourth port to document and firewall (API,
exec, telemetry, proxy).

**Why (security, simplicity).**
- (a) fails because tonic cannot upgrade, and because proxied pages would
  share the API's origin.
- (b) fails because that listener has no TLS (`src/manager/exec_ws.rs:56`),
  disappears when exec is off, and is the last origin an arbitrary workload
  page should share.
- The `OriginPolicy` exclusion matters on its own. Loopback origins on any
  port are trusted (`src/manager/origin.rs:58-63`), so on a dev cluster
  with ACLs off, a proxied page at `127.0.0.1:4750` could otherwise drive
  `:4747`.
- On by default, because the proxy does nothing without a grant, and
  AGENTS.md says to pick the default that is right for a new cluster.

## 6. Sessions are stateless and HMAC-signed with a keyring-derived key

**Call.** Codes and session cookies are signed claims under a key derived
from the cluster keyring. Nothing is written per session. A spent code is
remembered only in the memory of the manager that redeemed it. A session is
re-authorized against its token's accessor every 30 seconds.

**Alternatives.**
- (a) Session rows in raft. These allow listing sessions, revoking a single
  session, and codes that are single-use across every manager.
- (b) Signing with the workload-identity JWKS, so a future gateway could
  verify sessions offline.

**Trade-off.**
- Sessions can't be listed or revoked one at a time. Revoking the token is
  the kill switch.
- Behind a load balancer, a stolen code could be redeemed once on each
  other manager within its 60 seconds.
- HMAC keeps verification on managers, so the gateway needs a signing
  change (open question 4).

**Why (performance, simplicity).** Exec writes nothing to raft per session,
and opening a dashboard should not either. (b) would make the proxy depend
on `workloadIdentity.enabled`, which defaults to off. The keyring is always
present.

## 7. Phase 1 targets allocations and instances; service targets wait for Phase 3

**Call.** Phase 1 supports `alloc/<id>` and `instance/<id>`. An instance is
re-resolved to its current allocation for each upstream connection. The
**Open** button on a service-endpoint row uses that endpoint's allocation.
`service/<name>`, which pins a healthy endpoint per session, ships with the
Phase 3 port-forward.

**Alternatives.** Service targets first, since "open Grafana" is the most
natural phrasing.

**Trade-off.** In Phase 1, a non-instance service's URL changes when its
allocation is rescheduled.

**Why (simplicity).** A service target needs stickiness and health-based
re-picking, which is the start of a load balancer and a non-goal to grow.
Instances already give the motivating case (sandboxes and workspaces) a
URL that survives reschedules.
