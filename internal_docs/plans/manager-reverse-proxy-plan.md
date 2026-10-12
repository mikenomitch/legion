# Manager Reverse Proxy: open a workload's web UI through the manager

> **Judgement calls:** [manager-reverse-proxy-plan_calls.md](../calls/manager-reverse-proxy-plan_calls.md)

**Status: PLANNED** (written 2026-10-12). Nothing is built. Three phases, each
ships alone. Phase 1 works wherever exec works today (one host), and reaches
other hosts when [worker-plane-auth-plan.md](worker-plane-auth-plan.md)
Phase 1 lands, with no change here. Deliberately deferred: workers behind
NAT, HTTP/2 and gRPC upstreams, body rewriting, revoking a single session,
and serving these routes from `legion start gateway` (§6.6). Split out of
[operator-ui-gaps-plan.md](operator-ui-gaps-plan.md), which links a node's
public address when its ports are open. This plan covers nodes whose ports
are closed.

## 1. Problem statement

A job's web UI (Grafana, Jupyter, code-server in a sandbox, `/debug/pprof`)
listens on a dynamic port on some worker, and opening it in a browser needs
a route to that node and port. The UI links an endpoint's `address:port`,
or its node's `LEGION_WORKER_PUBLIC_ADDRESS` (operator-ui-gaps §6.6), but
both help only when the port is open to the browser. Real deployments
put workers in a private subnet open to managers only. So operators get a
VPN, run `ssh -L` to a port that moves on every reschedule, open the dynamic
port range, or run their own Traefik. `legion exec` plus `curl` reaches the
app but cannot render it. Every operator can already reach the manager with
credentials they hold, so the manager should carry the browser the rest of
the way.

## 2. Prior art

- **Kubernetes API server proxy**
  (`/api/v1/namespaces/{ns}/services/{name[:port]}/proxy/{path}`, RBAC on the
  `services/proxy` subresource,
  [docs](https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster-services/)).
  Browsers can't attach the token, so people run `kubectl proxy` locally.
  HTML bodies are rewritten for the subpath, and SPAs still break. A failed
  WebSocket upgrade once left an authenticated backend connection open for
  reuse (CVE-2018-1002105,
  [#71411](https://github.com/kubernetes/kubernetes/issues/71411)). *Take:*
  a subpath and a grant per target, unshared upstream connections, and
  `kubectl port-forward`'s shape (dial inside the pod's namespace, stream
  bytes) for our worker side and Phase 3. *Reject:* body rewriting.
- **Nomad** has no proxy and no port-forward. Its UI links `host:port`, and
  people add Traefik or Fabio off the Consul catalog, or a **Consul ingress
  gateway**. Those authorize service to service by intention, not by human:
  right for public ingress (a non-goal), wrong for "let this operator see
  this dashboard".
- **Coder workspace apps** ([docs](https://coder.com/docs)), the closest
  analog, have path apps (`/@user/ws/apps/code/`) and subdomain apps
  (`code--ws--user--apps.coder.example.com`). Coder recommends subdomains
  because path apps share an origin, and trades a short-lived dashboard
  token for a cookie on the app's host. Cloudflare Tunnel and ngrok share
  the shape: the edge authenticates and routes by hostname. *Take:* both
  modes, in that order, and the code-for-cookie exchange.
- **Apps under a subpath** need setup (Grafana `root_url`, Jupyter
  `base_url`, Prometheus `--web.external-url`), and SPAs with absolute
  `/assets/` paths break. *Take:* tell the task its prefix, and keep it
  stable.

## 3. Jobs to be done

- **Job author:** when my dashboard runs on a node I can't reach, I want to
  open it from Legion in one click, without a VPN or a firewall ticket.
- **On-call responder:** when one instance misbehaves, I want to open *that*
  instance's status page as it is now, before I restart it.
- **Cluster operator:** when workers sit in a private subnet, I want people
  to reach their apps without my opening inbound ports, and to see what the
  manager carries for whom, so it can't become ingress.
- **Security admin:** I want viewing a dashboard not to grant a shell, and
  who opened what in the audit log.
- **Autonomous agent:** I want to reach an HTTP endpoint on a private node
  by instance name, with the credential I already hold.

*Not in scope:* the app's own end users. That is public ingress.

**Scenarios.** (1) code-server runs in keyed instance `ws-7k2q` on a private
node. The operator clicks **Open → http** on the instance row and gets
`https://legion.corp:4750/proxy/i/ws-7k2q/http/`, which keeps working after
the instance is resumed elsewhere. (2) Grafana sets `root_url` from
`LEGION_PROXY_PATH_HTTP`. The responder clicks **Open** on its catalog row,
and revoking their token closes the tab's connections within 30 s.

## 4. Goals and non-goals

**Goals:** one click opens any declared port through the manager, with no
route from browser to node; WebSockets and streamed bodies work; access is
its own ACL grant, audited and re-checked like exec; a target's URL is
stable across sessions and reschedules; a CLI path needs no browser.

**Non-goals:** public ingress, custom domains, ACME (the "cluster-edge
ingress" TODO); load balancing; non-HTTP in the browser; body rewriting;
passing the Legion user to the app; workers a manager cannot dial; and
sandbox-platform throughput (the gateway is the scale path).

## 5. What exists today

Line numbers as of 2026-10-12. Several of these files are being edited.

- **Main listener**: tonic with `accept_http1(true)` and the chain
  `OidcHttpLayer` → `OriginGuardLayer` → CORS → `GrpcWebLayer` → metrics
  (`src/manager/daemon.rs:532-546`). `OidcHttpLayer` already serves plain
  HTTP GETs ahead of gRPC (`src/manager/oidc_http.rs:8-14`). tonic cannot
  upgrade (`daemon.rs:437`). Loopback origins on *any port* are always
  allowed (`src/manager/origin.rs:10-14`, `:58-63`).
- **Exec WebSocket on `+ 1`**, only when `exec.enabled`
  (`daemon.rs:439-457`): raw hyper with upgrades
  (`src/manager/exec_ws.rs:55-80`), an `Origin` check (`:117-135`), the
  bearer token in the first frame (`:195-208`), and a re-check every 30 s
  (`src/manager/acl.rs:626`). It has no TLS (`exec_ws.rs:56`), though the UI
  derives `wss://` for an `https://` manager (`ui/src/lib/execSocket.ts:18`).
  Telemetry is on `+ 2`. The UI is its own origin, holding its token in
  `localStorage` (`docs/auth-and-acls.md:216`).
- **Exec as a manager proxy**: a coarse `exec:write` check before lookup
  (`src/manager/exec.rs:60-76`), `exec_audit` (`:243-255`), and one worker
  dial per session (`:258`) to the manager-only `Worker.ExecTask`
  (`proto/legion.proto:7463`).
- **Addresses**: workers bind and register `127.0.0.1`
  (`src/worker/daemon.rs:177`, `:398-401`). `worker_address_host`
  (`src/storage/store.rs:230`) feeds `AllocNetwork.address` (`:12227`) and
  `ServiceEndpoint.address` (`:13588`), and the new `publicAddress` is
  display-only (`daemon.rs:82-100`). No manager can dial a worker on another
  host yet (worker-plane-auth §1.1).
- **Ingress last hop**: `127.0.0.1:<private port>` in the task's namespace
  (`src/worker/netd/ingress.rs:521-522`).
- **ACLs**: `exec`, `scale`, `boot`, and `snapshot` are separate resources
  so narrow grants stay narrow (`src/structs/acl.rs:14-66`). `check_acl` is
  at `src/manager/server.rs:1606`, and `tests/acl_endpoints.rs:1489`
  enforces coverage.
- **Adjacent**: `legion start gateway` will serve
  `https://<port>-<instance>.<domain>` over the worker plane
  (sandbox-exec-plan §6.6, sandbox-scaling-plan §6.14). Port forwarding and
  cluster-edge ingress are unplanned (`internal_docs/TODO.md:2634-2635`).

## 6. Proposed design

### 6.1 Surface

An **Open** menu, with one entry per declared port, goes on the pages
operator-ui-gaps §6.3 added (`/allocations/:id` ports and endpoints,
`/jobs/:job/instances/:id`) and on service-endpoint rows. The CLI mints the
same link:

```console
$ legion proxy url instance/ws-7k2q:http
https://legion.corp:4750/proxy/i/ws-7k2q/http/?legion-code=lpc_3f9…   (single use, 60s)
$ legion proxy url alloc/42
Error: allocation 42 declares ports `http` and `metrics`; name one, like alloc/42:http
$ legion proxy url alloc/42:http -j
{"ok":true,"data":{"url":"https://…","path":"/proxy/a/42/http/","allocationId":42,"expiresAt":"…"}}
```

Targets are `alloc/<id>`, `instance/<id>`, and `service/<name>` (Phase 3),
each with an optional `:<port label>`. The label can be left out when there
is one port, and a service defaults to its advertised port.

### 6.2 Browser auth: a one-time code for a target-scoped cookie

1. The UI calls `CreateProxySession { target }` over gRPC-Web. The manager
   authorizes `proxy:write`, resolves the target, and returns a URL with a
   signed code (60 s, single use).
2. The UI opens a tab on the click, so popup blockers allow it, and points
   it at the URL (`noopener`) when the reply arrives.
3. The proxy verifies the code and redirects (`303`) to the URL without it
   (`no-store`, `Referrer-Policy: no-referrer`). The redirect sets
   `legion-proxy=<session>; Path=/proxy/i/ws-7k2q/http/; HttpOnly;
   SameSite=Lax`, plus `Secure` under TLS.
4. Each request checks the cookie's signature, its expiry, and that its
   target matches the path. The proxy strips the cookie, forwards the
   request, and re-authorizes at most every 30 s. A revoked token or a
   removed grant closes open connections, WebSockets included.

The session is signed claims (id, target, token accessor id, expiry) under
an HMAC key derived from the cluster keyring. Nothing is written per
session, and any manager verifies any session. It lasts `proxy.sessionTtl`
(default `8h`), never past the token's expiry. A code is spent in the
memory of the manager that redeems it, so for its 60 s a stolen code could
still be redeemed once on each other manager (calls 6). Without a valid
cookie, every path gets the same `401`. With `proxy.uiUrl` set, that page
links to a UI route that mints a fresh session, so a bookmark costs one
click.

### 6.3 Routing: stable subpaths now, a host per target in Phase 2

Paths are `/proxy/a/<alloc>/<label>/`, `/proxy/i/<instance>/<label>/`, and
`/proxy/s/<service>/<label>/` (P3). A path names a target, not a session,
so an app can be configured with it (instance ids are `[A-Za-z0-9._-]`).
The proxy strips the prefix and sends it as `X-Forwarded-Prefix`, and the
task gets it at start as `LEGION_PROXY_PATH_<LABEL>` (the instance path if
there is one). **Only headers are rewritten:** `Location` and `Set-Cookie`
`Path` get the prefix, and `X-Forwarded-*` is added, with `Host` preserved.

**Every Phase 1 target shares one origin**, so a page from one proxied app
can read another app the same browser holds a session for. The public doc
says so, and recommends Phase 2 for untrusted code such as sandboxes.

**Phase 2**: `proxy.domain` with wildcard DNS and a cert (`proxy.tls`)
serves `https://http--ws-7k2q.proxy.legion.corp/`. Each target is its own
origin, needs no base path, and cannot read its neighbours. Chrome and
Firefox resolve `*.localhost` to loopback, so dev clusters get
`http://http--ws-7k2q.proxy.localhost:4750/` without DNS. An id that isn't
a DNS label gets a short hash label.

### 6.4 Listener: its own port, never a trusted origin

`proxy.port` defaults to `server.port + 3` (4750). The listener is hyper
(HTTP/1.1, HTTP/2, upgrades), with TLS from `server.tls` unless `proxy.tls`
is set. It is on by default, and `proxy.enabled = false` turns it off.
`OriginPolicy` must refuse the proxy's own origins on the gRPC-Web guard
and the exec WebSocket, even on loopback: `proxy.publicUrl`, any loopback
host on `proxy.port`, and `*.<proxy.domain>`. Otherwise, on a dev cluster
with ACLs off, a proxied page at `127.0.0.1:4750` passes the loopback rule
and drives the cluster at `:4747`.

### 6.5 Transport, targets, and limits

`Worker.ProxyTunnel(stream ProxyFrame) returns (stream ProxyFrame)` carries
one TCP connection per stream: `ProxyOpen { allocation_id, port_label }`,
then bytes, then close. The streams multiplex over the manager's one HTTP/2
connection to the worker.

The worker admits only the `manager` caller class (a new row in
worker-plane-auth §6.1). It refuses unless the allocation is local, running,
and declares the label, then dials `127.0.0.1:<bind_port>` in the
allocation's namespace with the ingress datapath's `netns::connect_tcp`,
never a caller-named address. The manager runs a hyper HTTP/1.1 client over
each tunnel stream. A `101` becomes a splice, and bodies stream with
backpressure. Connections are pooled per session, never shared, and closed
when an upgrade fails.

| Target | Resolves to | For |
| --- | --- | --- |
| `alloc/<id>` | that allocation; `503` once it stops | exact debugging |
| `instance/<id>` | its current allocation, re-resolved per upstream connection | sandboxes and replicas; survives reschedule and resume |
| `service/<name>` (P3) | one healthy endpoint, pinned for the session until it turns unhealthy | "open Grafana" |

| Limit | Default | On breach |
| --- | --- | --- |
| Worker dial / response headers | 5 s / 60 s | `502` naming node and port / `504` |
| Idle both ways, WebSockets too (`proxy.idleTimeout`) | 15 min | closed |
| Upstream connections per session / manager (`proxy.maxConnections`) | 64 / 2048 | `503` + `Retry-After` |
| Request headers / bodies | 64 KiB / streamed, unlimited | `431` / none |

Error pages are HTML, or the standard error envelope under
`Accept: application/json`. A stopped instance's page names
`legion instance start <id>`.

### 6.6 ACL, audit, and the gateway

`proxy` is a new resource. `write` opens a session (`CreateProxySession`,
and `ProxyConnect` in Phase 3), scoped by job name and namespace like
`exec`. There is no `read`.

```nickel
[ { resources = ["proxy"], actions = ["write"], names = ["grafana", "ws-pool"] } ]
```

A coarse `proxy:write` check on `AclScope::any()` runs before the target is
resolved and the precise one after, so unknown and forbidden targets look
the same. The proxy enters at the private port, behind the ingress chain,
as exec enters the process. The doc says `proxy:write` on an app that runs
code (Jupyter) equals `exec:write`.

Session creation logs `proxy_audit` (principal, accessor id, target,
allocation, node, job) and emits `proxy.session_created`. Metrics count
sessions, open connections, bytes, and upstream errors. Codes, cookies, the
app's `Authorization` and `Cookie`, and query strings are never logged,
because Jupyter's `?token=` is a secret.

`legion start gateway` (the sandbox plans) is the scale-out version. This
plan builds what it needs: `Worker.ProxyTunnel`, the host grammar, and the
session cookie.

## 7. Proto, struct, config, and persistence changes

- **Proto**: Manager `CreateProxySession` (P1) and
  `ProxyConnect(stream ProxyFrame) returns (stream ProxyFrame)` (P3), and
  Worker `ProxyTunnel`. Messages: `ProxyTarget { oneof { allocation_id,
  instance_id, service }, port_label }`, `CreateProxySessionReply { url,
  path, allocation_id, expires_at }`, and `ProxyFrame { oneof { open, data,
  close } }`, all with full proto comments.
- **Manager config**: `proxy { enabled, port, publicUrl, uiUrl, sessionTtl,
  idleTimeout, maxConnections }`, plus `domain` and `tls` in P2.
  `publicUrl` defaults to `server.advertiseAddr`'s host on `proxy.port`.
- **Also**: env `LEGION_PROXY_PATH_<LABEL>` and `AclResource::Proxy`.
  Nothing is persisted, and there are no job-config fields unless open
  question 1 adds one (then into `configs/jobs/maximal.ncl`). `catalog()`
  gets `proxy url` (`mutates: false`, `read_only_safe: false`, since it
  hands out write-capable access), and both RPCs get `acl_endpoints` rows.

## 8. Implementation phases

**Phase 1: browser proxy on a subpath.** The listener, sessions,
`CreateProxySession`, `Worker.ProxyTunnel`, `alloc` and `instance` targets,
WebSockets, header rewriting, the `proxy` resource, audit, metrics, the
`OriginPolicy` exclusion, the UI **Open** menus, `legion proxy url`, and
`LEGION_PROXY_PATH_<LABEL>`. Docs: a new `docs/manager-proxy.md`, plus
`auth-and-acls.md`, `ui-architecture.md`, `agent-readiness.md`, and
`architecture-managers-workers.md`. Useful on one host at once, and on
every node after worker-plane-auth Phase 1.

**Phase 2: a host per target.** `proxy.domain`, `proxy.tls`, `*.localhost`,
DNS-label hashing, and the UI preferring hosts. Closes the shared-origin
caveat.

**Phase 3: port-forward and service targets.** `legion proxy <target>
[--local-port N]`, an L4 forward over `Manager.ProxyConnect` that carries
any TCP protocol, plus `service/<name>` targets and the MCP tool.

## 9. Testing plan

- **Unit**: target parsing; code and cookie signing (expiry, single use,
  target binding); header rewriting and stripping; `OriginPolicy` refusing
  `127.0.0.1:4750` but allowing `:5173`; the worker refusing undeclared
  labels and non-local allocations; one `401` for unknown and forbidden.
- **Integration, `tests/manager_proxy.rs`**: an in-process manager and
  worker, with a task serving HTTP and a WebSocket echo (like the ingress
  tests' `task_on`). The test follows a code to a `303`, a cookie, and a
  `200`, then checks a prefixed redirect, the WebSocket echo, denial with
  `exec:write` but no `proxy:write`, the WebSocket closing after token
  revocation (with a shortened re-check), and the instance URL surviving a
  new allocation. The `acl_endpoints` rows keep
  `every_manager_rpc_is_classified` green.
- **Manual**: after worker-plane-auth Phase 1, Grafana and Jupyter on a
  private-subnet worker. For Phase 2, a wildcard domain and `*.localhost`.

## 10. Rollout and backward compatibility

Upgraded managers open `4750` by default. Existing policies don't name
`proxy`, so client tokens can't proxy until granted it, while management
tokens and ACL-off clusters can, as with exec. Old workers answer
`Unimplemented`, and the error page names the node. Nothing is migrated.

## 11. Open questions

1. **Keep the prefix for apps that expect it?** Jupyter's `base_url` wants
   the full path. Lean: add a per-port `proxy = { prefix = "keep" }` if
   Phase 1 testing confirms it is needed. Phase 2 makes the question moot.
2. **Upstream `Host`.** Vite and Jupyter reject unknown hosts. Lean:
   preserve it, and document each app's allow-list setting.
3. **MCP.** Lean: a one-shot `legion_proxy_request` tool in Phase 3, with
   `mutates: true`, behind `--allow-write`.
4. **Gateway alignment.** Its hosts are `<port>-<instance>`, and HMAC
   sessions verify only on managers. Lean: accept a label or a task-side
   port in the host, and sign sessions asymmetrically (audience
   `legion:proxy`) once a gateway needs to verify them offline.

## 12. Acceptance criteria

- **Phase 1**: **Open** on an instance renders its app (WebSockets included)
  through a manager while the worker accepts nothing from the browser. A
  token without `proxy:write` is refused, and unknown and forbidden targets
  look alike. Revocation closes the tab's connections within 30 s. Every
  session leaves a `proxy_audit` line and an event, with no code or cookie
  in any log. A proxied page cannot call the gRPC-Web API or exec.
- **Phase 2**: each target has its own host, an app with absolute `/assets/`
  paths renders, and no target's page can read another's responses.
- **Phase 3**: a local forward carries HTTP and non-HTTP TCP, and a service
  session stays on one endpoint until that endpoint turns unhealthy.
