# Ingress: a bundled app that puts services on the internet

> **Judgement calls:** [ingress-app-plan_calls.md](../calls/ingress-app-plan_calls.md)

**Status: IN EXECUTION** (written 2026-11-07, revised 2026-11-08). Phases 1
and 2 are built in the same change as this plan. Phase 3 is planned and
nothing in it is built. Where the build differs from this text, the calls
doc says how and why.

**Revised 2026-11-08: certificates come from the cert manager.** The
in-app ACME client of §6.5 was built, then replaced before merge by
delegation to the bundled cert manager (`certs`, cert-manager plan): the
ingress keeps one `certs/Certificate` per routed host and answers HTTP-01
from the Issuer's thumbprint (§6.7, calls entry 1). §6.7 wins where it and
§6.2 or §6.5 disagree; those sections stay as written, marked. The same
revision added where to point DNS (§6.8). A later revision the same day made the
ingress run one replica on every node it is placed on (`replicas =
"per-node"`; [app-per-node-replicas-plan.md](app-per-node-replicas-plan.md)),
which replaces §6.3's single replica and `--replicas N` advice. Phase 2's Pebble check (§12) is
replaced by an end-to-end test against the cert manager and its fake ACME
CA, which validates HTTP-01 through the ingress.

Deliberately deferred:

- a catalog watch stream (endpoints are polled)
- HTTP/2 to backends, and gRPC
- per-namespace host ownership
- TCP and TLS passthrough routes
- waking a suspended or scaled-to-zero service on a request
- metrics and access logs
- a load benchmark, and a connection limit

---

## 1. Problem statement

Legion runs a web service in one block and finds it in another. It still
cannot put that service on the internet. Today a team that wants
`https://shop.example.com` to reach a Legion service has to do all of this:

1. **Run its own proxy.** Write an nginx, Caddy, Traefik, or HAProxy job.
   Pin it to a node with a static port. Keep its config in step with the
   catalog: a `connect.templates` file rendered from the endpoint feed, a
   cron that runs `legion service resolve`, or a Consul-style sync. Nothing
   watches the catalog for an outside proxy (`internal_docs/TODO.md`, "A
   watchable catalog for outside load balancers"), so each of these ways
   polls.
2. **Get certificates.** Legion does not run an ACME client
   (`docs/network-handlers.md`, "Certificates and CAs from KV"). The
   suggested path is a `lego` renewer job (`configs/jobs/cert-renewer.ncl`)
   that writes PEM bundles into KV, plus a `certFrom` reference on every
   port that should serve them.
3. **Bind ports 80 and 443.** An `exec` task that does not run as root
   cannot bind them. The other options each cost something:
   - run the proxy as root, which is refused in `legion-system` and unwise
     anywhere else;
   - put the datapath in front with an ingress chain, which hides the
     client's address from the proxy (`docs/network-handlers.md`, "Ingress":
     "Legion does not pass the original peer to the task");
   - use a container and its own network namespace.

Each of these is a weekend, and each is a different weekend for every team.
The TODO lists the gap twice: "Ingress proxy / load balancer" under the
ecosystem apps, and "Cluster-edge ingress load balancer" under Networking. The
apps plan (`legion-apps-plan.md` §1) names "an ingress" among the add-ons
the app framework exists to make simple.

## 2. Prior art

- **Kubernetes Ingress / Gateway API.** An `Ingress` object maps
  host and path to a Service. A separately installed controller (ingress-nginx,
  Traefik, Envoy Gateway) implements it, and cert-manager is another install
  with its own CRDs (`Certificate`, `Issuer`, `ClusterIssuer`). This is three
  products and four object kinds before the first HTTPS request. The Gateway
  API splits who owns the listener (Gateway) from who owns the route
  (HTTPRoute), which is right for large multi-tenant clusters and heavy for
  everyone else.
  *Take:* host plus path-prefix routing to a service is the right model.
  *Reject:* a separate route object and a separate cert object. The route
  belongs on the service it routes to.
- **Nomad + Traefik/Fabio + Consul.** Tags on a Consul service
  (`traefik.http.routers.shop.rule=Host(...)`) drive a proxy that watches
  the catalog. Routing next to the service is good, and it is why Nomad users
  like it. Routing written as stringly-typed tags with no validation is bad:
  a typo is silently a 404. *Take:* routing declared on the service. *Reject:*
  unvalidated tags. Legion's jobspec extensions validate at submit.
- **Caddy.** Automatic HTTPS by default: every site name gets a Let's
  Encrypt certificate with no configuration, and HTTP redirects to HTTPS.
  Caddy agrees to the CA's terms on the operator's behalf, and its docs say
  so. *Take:* all of it. This is the bar "incredibly easy to set up" sets.
- **Fly.io / Render / Heroku.** "Add a domain, point DNS at us, done." The
  user never sees a proxy. *Take:* that experience is the goal. *Reject:*
  nothing; they are what this is measured against.
- **Envoy / HAProxy load balancing.** Power-of-two-choices on outstanding
  requests is Envoy's `LEAST_REQUEST` default. It is cheap and avoids the
  herding that strict least-connections causes across replicas. *Take:* P2C
  plus passive outlier ejection.

## 3. Jobs to be done

**App developer / job author**
- When my web service is deployed, I want it reachable at my domain over
  HTTPS by adding one line next to the service, so I don't have to learn a
  proxy, a certificate tool, or DNS-01.
- When I typo a hostname or pick one another team already serves, I want the
  deploy to tell me then, so I don't find out from a 404 in production.
- When I roll out a new version, I want traffic to follow the healthy
  instances without dropped requests or a config reload, so deploys stay
  boring.
- When my service is a WebSocket app or my API lives under `/api`, I want
  that to work without special plumbing.

**Cluster operator**
- When I set up a cluster, I want to turn on public HTTP routing with one
  command and get certificates without managing them, so the cluster is
  useful on day one.
- When I already have certificates (a wildcard, a corporate CA), I want to
  hand them over once, so the ingress serves them instead of asking Let's
  Encrypt.
- When I run several managers, I want the ingress on more than one of them,
  so losing a node does not take every site down.

**On-call responder**
- When a site returns errors, I want one place that lists every route, its
  backends' health, and its certificate's state, so I can tell DNS, the
  certificate, and the service apart.

**Security / platform admin**
- When the ingress holds certificate keys and a broad read grant, I want it
  to run unprivileged, with exactly the rights it needs, keys in the
  reserved KV prefix, and nothing else, so a bug in the proxy cannot become
  root or reach another tenant's secrets.

**Autonomous agent**
- When I deploy a service for someone, I want to set its public hostname in
  the jobspec and read the route's state back as structured data (`legion
  app view ingress … -j`), so I can confirm the site is live without
  scraping.

*Not in scope:* tenant-level host ownership (which namespace may claim which
domain). Phase 3 covers it. Until then the first claim wins, the deploy that
would take a claimed host is refused, and the public doc says so.

### User scenarios

1. **The single-box startup.** One manager runs on a VM with a public IP.
   The operator runs `legion app enable ingress`. The developer adds
   `ingress = { host = "shop.example.com" }` to the `web` service and
   deploys. They point an A record at the VM. Within a minute
   `https://shop.example.com` serves the site with a Let's Encrypt
   certificate, and `http://` redirects to it.
2. **Three managers behind round-robin DNS.** The operator runs `legion app
   set ingress --replicas 3`. Static ports place one replica per manager.
   Every replica serves every route. One replica orders certificates, and
   all of them serve them, from the app's KV. A manager reboot costs
   nothing: DNS clients retry the other addresses, and the replica comes
   back where it was.
3. **The corporate wildcard.** An operator writes the company's
   `*.corp.example.com` bundle to `legion/apps/ingress/certs/corp` once.
   Every service under that domain is served with it, and the ingress asks
   no CA for those names.

## 4. Goals and non-goals

**Goals**
- One line on a service routes a hostname (and optionally a path prefix) to
  it, validated at submit.
- Automatic HTTPS: certificates ordered, stored, shared by every replica,
  and renewed with no configuration. HTTP redirects once a certificate
  exists.
- Off by default, on with one command. It claims ports 80 and 443 on the
  nodes it runs on, which no cluster should do by surprise.
- Fast on the request path: no extra hop in front of the proxy, pooled
  backend connections, HTTP/2 to clients, and route lookup without locks
  held across awaits.
- The real client address reaches the backend (`X-Forwarded-For`).
- Unprivileged: runs as the app's own uid, holding only
  `CAP_NET_BIND_SERVICE`.

**Non-goals**
- A general-purpose proxy configuration language (rewrites, header rules,
  auth, rate limits). Use a proxy job, or a `network.ingress` chain on the
  service.
- Being a service mesh or replacing `connect`.
- Routing to `mesh = true` services. Their ports take only node-to-node
  mTLS, and the app has no node certificate. This is reported, not
  silently broken.
- Cluster-external load balancers. The ingress is the edge. A cloud load
  balancer can sit in front of it, with ACME off.

## 5. Background: what exists today

- **The app framework** (`src/apps/mod.rs`). A bundled app is one `BUNDLED`
  entry: a Nickel manifest (`crates/common/src/bundled_apps/*.ncl`) and a body,
  `fn(AppContext) -> AppFuture`. The first Control leader seeds a record
  for each, enabled (`seed_record`, `crates/manager/src/manager/apps.rs:363`). The manager
  config's `apps.<name>.enabled` overrides that at seed time only. The
  reconciler renders the job (`render_app_job`, `crates/common/src/structs/app.rs:1389`)
  with the app's uid, its config file, and its `legion` identity.
- **Jobspec extensions** (`docs/apps.md`, "Jobspec extensions"). An app
  declares a key at the `service` point with a JSON Schema. The manager
  checks values at submit, and with `validate = true` asks the app
  (`UiRegistry::validate`, `src/apps/ui.rs:326`). The app reads values over
  `WatchAppExtensions` (`proto/legion.proto:2921`): a snapshot, then
  upserts and deletes. A disabled app's keys are still checked. A disabled
  or unreachable app passes values with a warning. No bundled app declares
  extensions yet, and there is no Rust client helper for the watch.
- **The catalog** (`docs/service-discovery.md`). It is derived, not
  registered. `ResolveService` (`crates/manager/src/manager/server.rs:11452`) returns
  healthy endpoints filtered by `job:read`. There is no watch stream, so a
  consumer outside the worker polls.
- **The gateway** (`src/gateway.rs`). It reads one HTTP/1 head per
  connection and splices the rest. It is instance-keyed (wake on request)
  and makes one routing decision per connection. That is wrong for path
  routing, where a browser reuses a connection across `/` and `/api`, so
  the ingress needs a per-request proxy. The gateway's envelope and
  host-parsing helpers are worth sharing.
- **The datapath's HTTP mode** (`crates/worker/src/worker/netd/http.rs`). It is a hyper 1
  proxy with a private connection pool for a task's own upstreams. It is
  tied to the datapath's listener context, so the ingress reuses its
  approach, not its code.
- **TLS helpers** (`crates/worker/src/worker/netd/tls.rs`): `certs_from_pem` and
  `key_from_pem`, with rustls 0.23 on `ring`. `rcgen` 0.13 (CSRs),
  `x509-parser` 0.16, `ring` (ES256), and async `reqwest` (rustls) are
  already in the tree, so an ACME client needs no new crate.
- **Exec hardening** (`crates/worker/src/task_plugins/exec_isolation.rs`). It can only
  *drop* bounding-set capabilities. The pre-exec sequence
  (`configure_linux_process_options`, `crates/worker/src/task_plugins/exec.rs:729`)
  already owns the user switch when hardening needs privilege, which is
  where an ambient capability has to be raised.
- **Static ports** are scheduler-reserved, one holder per node
  (`task_static_ports`, `crates/manager/src/manager/scheduler.rs:1051`). So `replicas = N`
  with static 80/443 spreads one replica per node without new placement
  code.

## 6. Proposed design

### 6.1 The surface a job author types

```nickel
{
  name = "shop",
  task = {
    name = "web",
    runtime = "container",
    config = { image = "ghcr.io/acme/shop:1.4" },
    network = { ports = [{ name = "http", to = 8080 }] },
    service = {
      port = "http",
      ingress = { host = "shop.example.com" },
    },
  },
}
```

The `ingress` key, at the `service` extension point:

| Field | Default | Meaning |
| --- | --- | --- |
| `host` | required | A hostname, or a list of up to 16. `*.example.com` matches one label. Lower-cased. |
| `path` | `"/"` | A path prefix, matched on segment boundaries (`/api` matches `/api` and `/api/x`, not `/apix`). |
| `stripPath` | `false` | Remove `path` before forwarding (`/api/users` reaches the service as `/users`). |

The schema rejects a malformed host, a path that does not start with `/`,
and unknown fields at submit. Because the key sets `validate = true`, the
running app also refuses a `(host, path)` another service already holds:

```text
shop: task web: service web: ingress (ingress app): shop.example.com/ is already routed to storefront (task web, service web)
Hint: the app refused these values; fix them and submit again
```

When the app is disabled, the manager's generic warning says the value was
not checked, and the docs tell the reader to run `legion app enable
ingress`.

### 6.2 The surface an operator types

```console
$ legion app enable ingress
$ legion app status ingress
NAME     VERSION  SOURCE   STATUS   PLACEMENT  REPLICAS  UI        DETAIL
ingress  0.1.0    bundled  running  managers   1/1       nav, job  on legion-system-x8f2k1qz
$ legion app set ingress --replicas 3          # one per manager (static ports)
$ legion app view ingress nav                  # every route, backends, certificate
$ legion app config set ingress -f ingress.json
```

> **Superseded by §6.7** for the certificate fields: the config is now
> `certificates`, `issuer`, `acme { server, email }`, and `redirectHttps`.

The app's config. Every field is optional:

```json
{
  "acme": {
    "enabled": true,
    "directory": "letsencrypt",
    "email": "ops@example.com",
    "trustRoots": "-----BEGIN CERTIFICATE-----..."
  },
  "redirectHttps": true
}
```

- **`acme.enabled`** (default `true`): order certificates for routed hosts.
- **`acme.directory`**: `letsencrypt` (the default), `letsencrypt-staging`,
  or a directory URL (step-ca, Pebble, ZeroSSL).
- **`acme.email`**: the account contact.
- **`acme.trustRoots`**: extra PEM roots for reaching a private directory.
- **`redirectHttps`** (default `true`): answer HTTP with a `308` to HTTPS
  for a host that has a certificate.

A manager config can seed it on: `apps = { ingress = { enabled = true } }`.

### 6.3 Where it runs, and ports 80 and 443

The manifest:

- runs `legion:self app run ingress` as an `exec` task;
- declares `network.ports = [{ name = "http", static = 80 }, { name =
  "https", static = 443 }, { name = "api" }]`;
- sets `hardening.capabilities.add = ["net_bind_service"]`.

It is seeded with `placement = "managers"` and `replicas = 1`.

**`hardening.capabilities.add`** is new to the exec runtime, and it takes
only `net_bind_service`. On Linux the pre-exec sequence does this:

1. `PR_SET_KEEPCAPS` before the user switch;
2. after the switch, `capset` to exactly `{NET_BIND_SERVICE}` (effective,
   permitted, inheritable);
3. `PR_CAP_AMBIENT_RAISE`, so the capability survives `exec`.

Every other capability is gone from the permitted set before `exec`. A
failure in any step fails the start, so a task never gets more than it
asked for. On macOS, binding below 1024 needs no privilege since 10.14,
and the field is a no-op. Any other name is a submit error:

```text
Error: exec hardening: capabilities.add may name only net_bind_service, which lets a task bind ports below 1024 without root; "sys_admin" would make it root-equivalent
```

Why not the datapath's root-owned listener? See calls entry 2: it costs a
hop and hides the client's address.

**Bundled apps can be seeded disabled.** `BundledApp` gains `seed_enabled`.
The autoscaler stays `true` and the ingress is `false`. The manager config's
`apps.<name>.enabled` still overrides.

### 6.4 Inside the app

```
WatchAppExtensions ──► route specs ──► compiled RouteTable ─┐   (Arc swap)
                                                            ├─► proxy (hyper, :80 / :443)
ResolveService (every 2s, per routed service) ─► backends ──┘
ListKv legion/apps/ingress/ (every 10s) ─► certificate store ─► rustls SNI resolver
ACME issuer (KV lock holder only) ─► legion/apps/ingress/acme/certs/<host>
```

- **Routes.** The extension watch is a snapshot plus deltas, and it
  reconnects with backoff. Each value names its job, task, and service.
  The table maps exact hosts to path routes, longest prefix first, with a
  second map for `*.` wildcards. It is rebuilt on every change and
  published as an `Arc`: a request clones it and never waits on a writer.
  Two values with the same `(host, path)` are a conflict. The lower
  `(job, task, service)` wins, deterministically on every replica, and the
  loser is reported.
- **Backends.** One per `(job, task, service)`. Every 2 seconds the app
  resolves each service. Several resolves run at once, capped at 16 in
  flight. The app keeps only the declaring job's and task's endpoints, so
  two jobs that share a service name do not cross. Endpoint state
  (in-flight count, ejection) is kept by address across refreshes. If the
  manager is unreachable, the last good set stays (fail static). A
  `mesh_tls` endpoint is skipped and reported.
- **Balancing.** Power of two choices on in-flight requests. An endpoint
  whose dial fails is ejected for 10 seconds unless it is the last one.
- **Proxying.** The client side is hyper's auto server: HTTP/1.1 and HTTP/2
  (h2 by ALPN on TLS, prior knowledge on plaintext). The backend side is
  HTTP/1.1 over a per-address pool of `SendRequest`s, which are checked in
  when a response body ends. The app strips hop-by-hop headers and sets
  `X-Forwarded-For` (appended), `X-Forwarded-Proto`, and
  `X-Forwarded-Host`, and keeps `Host`. Retries are safe: a dial failure
  retries another endpoint before the request is consumed, and a pooled
  connection that closed under it gets the request back through
  `try_send_request`. WebSocket and other HTTP/1.1 upgrades take an
  unpooled connection and are copied both ways after the `101`.
- **Errors.** `404` (no route for the host or path), `502` (the backend
  failed), `503` (no healthy endpoint), `504` (no response headers within
  60 seconds). Each comes in the standard JSON envelope with a
  `Legion-Ingress-Error` header naming the code.

### 6.5 Certificates

> **Superseded by §6.7** for ordering: the ingress no longer runs an ACME
> client, account, challenge board, or issuer lock. Serving (SNI from KV),
> operator bundles, and redirects are unchanged.

- **Where.** The ingress's own reserved KV prefix:
  - `legion/apps/ingress/certs/<anything>`: operator bundles, which win;
  - `legion/apps/ingress/acme/certs/<host>`: ordered by the app;
  - `legion/apps/ingress/acme/account`: the ACME account key and URL;
  - `legion/apps/ingress/acme/challenges/<token>`: in-flight HTTP-01
    answers;
  - `legion/apps/ingress/replicas/<allocation>`: each replica's address,
    for the view.

  The bundle format is the one ports already use: the chain, leaf first,
  then the key.
- **Serving.** Every replica polls `ListKv` without values every 10
  seconds and fetches only the entries whose version changed. A
  certificate's names come from its leaf's SANs. SNI chooses an exact name
  first, then the `*.` parent. With no match, the handshake is refused.
- **Ordering.** Every replica runs the issuer loop. Only the holder of the
  KV lock `legion/apps/ingress/acme/lock` (60 s lease) acts:
  - It looks for routed exact hosts with no certificate, or whose
    certificate has less than a third of its lifetime left.
  - It skips a host covered by an operator bundle, and names no public CA
    could issue for: no dot, an IP literal, `localhost`, `.local`,
    `.internal`, `.test`, `.example`, `.invalid`, `.home.arpa`.
  - It orders one certificate per host over HTTP-01. The challenge answer
    goes to KV first, so whichever replica the CA reaches answers it.
  - It stores the bundle, then deletes the challenge.
  - After a failure it backs off per host: 5 minutes, doubling to 6 hours.
    That stays inside Let's Encrypt's failed-validation limit.
- **The ACME client** is RFC 8555 with ES256 JWS (`ring`), a P-256 account
  key, CSRs from `rcgen`, and async `reqwest`. It handles nonces (including
  one `badNonce` retry), polling with `Retry-After`, and problem documents
  carried into the reported error. No new dependency.
- **Redirects.** Plain HTTP for a host with a served certificate gets a
  `308` to `https://`. ACME challenges are never redirected.

### 6.6 Seeing it

Views (`legion-view/1`), each needing `job:read`:

- **`nav`: "Ingress".** Stats (routes, hosts, certificates), where the app
  listens (from the replica records), the routes table (host and path,
  target, healthy/total, certificate state, problems), and certificates
  (names, source, expiry, last ACME error).
- **`job`: "Ingress".** The job's own routes with their URLs. Hidden for a
  job with none.

`/healthz` reports the route and certificate counts. It is unhealthy only
when a listener could not be bound. A manager outage keeps the last routes
serving.

### 6.7 Certificates from the cert manager (revision)

The cert manager (cert-manager plan) shipped a general Issuer/Certificate
model on the same cluster, and two ACME clients in one product is the
duplication the revision removes. The ingress becomes a client of it:

- **One Certificate per provable routed host**, in `legion-system`, named
  `<host-slug>-<10 hex of sha256(host)>`, labeled `ingress/managed=true`
  and `ingress/host=<host>`, with `kv =
  legion/apps/ingress/issued/<name>`. Hosts the Issuer cannot prove are
  skipped with a reason; hosts an operator bundle covers are skipped. A
  route that goes takes its Certificate and its KV key with it.
- **The Issuer.** Config `issuer`, default `ingress`. For `ingress`, the
  app keeps the Issuer itself from `acme { server, email }`: ACME with
  `http01 = {}`, `namespaces = ["legion-system"]`. Any other name must
  exist and serve `legion-system`; the views say when it does not.
- **HTTP-01** is answered on `:80` from the Issuer's status `thumbprint`
  with `certs::http01::answer`, the same stateless responder `legion cert
  http01` uses. Every replica re-reads the Issuer every 5 s until it has the
  thumbprint, then every pass.
- **No order before the account.** With an HTTP-01-only Issuer and no
  thumbprint yet, new Certificates wait, so the first order cannot fail its
  challenge into the cert manager's 5-minute backoff.
- **One writer** by the KV lock `legion/apps/ingress/certificates/lock`
  (90 s lease), renewed every pass (30 s, and 2 s after routes settle).
  Every replica reads; only the holder applies and deletes.
- **Serving** is §6.5's KV store, reading `issued/` beside the operator's
  `certs/`; operator bundles still win.
- **Grants.** The ingress gains `object` read/write on
  `certs/Certificate` in `legion-system`, read on `certs/Issuer`, and write
  on the `certs/Issuer` named `ingress`. The cert manager's `via =
  "objects"` KV rule names `legion/apps/*` beside `*`, so acting for the
  ingress's Certificate it can write the ingress's private prefix; the
  for-object authority (what the Certificate's writer was checked for)
  still bounds it to keys that writer holds.
- **Config surface.** `acme.enabled` → `certificates`;
  `acme.directory` → `acme.server` (the cert manager's spelling);
  `acme.trustRoots` is gone (a private ACME CA is an Issuer of your own).

### 6.8 Where to point DNS (revision)

The quick start's weakest step was "point DNS at the node's address",
which the views showed as a cluster address only. Now:

- The ingress's job declares a catalog service, `legion-system/ingress`
  (port `http`), so `legion service resolve legion-system/ingress` lists
  every replica with its `publicAddress` (from
  `LEGION_WORKER_PUBLIC_ADDRESS`).
- The app resolves its own service every 10 s and shows **Point DNS here**
  on its page: the addresses, public first, and how to give a NAT'd node a
  public address.
- It resolves each exact routed host from its node (at once for a new host
  or a changed replica set, and every 60 s), and the Routes table's DNS
  column reads `points here`, `points to <ips>`, or `no record`.

## 7. Proto, struct, config, and persistence changes

- **No proto change.** Everything rides existing RPCs: `WatchAppExtensions`,
  `ResolveService`, KV, KV locks, and (§6.7) `ApplyObject`, `GetObject`,
  `ListObjects`, and `DeleteObject`.
- **No store change.** The app's records are KV under its reserved prefix.
- **Exec runtime:** `hardening.capabilities.add`, validated, honoured on
  Linux. It goes in `configs/jobs/maximal.ncl`, `docs/task-runtime-exec.md`,
  and `docs/task-supervision.md`.
- **Apps:** `BundledApp.seed_enabled`, and a new bundled manifest
  `crates/common/src/bundled_apps/ingress.ncl` with the `ingress` key at the `service`
  point.
- **Jobspec:** `service.ingress` (an app key, registered on every cluster by
  seeding). It goes in `configs/jobs/maximal.ncl` if the offline validator
  accepts bundled keys, and in a new `configs/jobs/ingress-demo.ncl`.
- **Cargo:** hyper `http2`, and hyper-util `server-auto`, `server-graceful`,
  and `http2`. These are features of crates already in the tree; `h2` is
  already locked through tonic.

## 8. Implementation phases

**Phase 1: routing (ships alone, useful behind a TLS-terminating load
balancer or with `acme.enabled = false`)**
- `hardening.capabilities.add = ["net_bind_service"]` in the exec runtime.
- `seed_enabled` for bundled apps.
- The `ingress` manifest, key, and validate handler.
- The route watch, the backend refresher, and the proxy on `:80`.
- Views, docs, and tests.

**Phase 2: HTTPS (ships alone on top of Phase 1)**
- `:443` with the SNI store from KV.
- ~~The ACME client and issuer, with lock, backoff, and renewal.~~ Revised:
  a Certificate per host from the cert manager, with the lock (§6.7).
- The redirect.
- Certificates in the views.
- Where to point DNS (§6.8).
- Docs, and an end-to-end test against the cert manager (replaces Pebble).

**Phase 3: planned**
- A `WatchServices` stream to replace polling, so a deploy's new endpoints
  route within a heartbeat.
- Per-namespace host ownership in the app config
  (`domains = [{ namespaces, hosts }]`), enforced at validate and at
  serve.
- HTTP/2 and gRPC to backends (`meta.protocol = "h2c"`).
- Metrics on the `api` port, and an NDJSON access log.
- Waking suspended keyed instances and scale-from-zero services on a
  request (the autoscaler's reserved `wake` source).
- TCP and TLS passthrough routes.
- ~~DNS-01 and wildcard certificates.~~ Available through §6.7: name a
  DNS-01 Issuer in `issuer`.
- HSTS.

## 9. Testing plan

- **Unit:**
  - route compile, match, and conflicts;
  - host and path normalization and the schema's examples;
  - header rewriting and hop-by-hop stripping;
  - the P2C pick and ejection;
  - JWS signing, the RFC 7638 thumbprint, key authorization, and CSRs;
  - SNI resolution with exact, wildcard, and operator-over-ACME precedence;
  - config parsing;
  - the manifest (permissions, ports, key, seed-disabled);
  - exec capability validation.
- **Linux, root only:** a child spawned through the exec pre-exec path as
  an unprivileged uid binds port 80 with `add`, and fails without it.
- **Integration (`tests/integration/ingress.rs`):**
  - the proxy core against real hyper backends and a fake route and
    endpoint source: host and path routing, `stripPath`, balancing across
    backends, retry past a dead endpoint, a WebSocket upgrade, h2 to the
    client, the redirect, and TLS with an operator bundle;
  - a real in-process manager: the bundled app is seeded disabled, its key
    validates at submit, and its values reach the app's identity over
    `WatchAppExtensions`;
  - ~~ACME end to end against an in-process CA, and against Pebble.~~
    Revised: against a fake control, a Certificate per host (skipped
    wildcard and internal names, removal with the route, the account gate),
    a named Issuer, HTTP-01 from the thumbprint, and the DNS check; and in
    `tests/integration/certs.rs`, the real cert manager engine issuing for
    the ingress through the cert manager's fake ACME CA, which validates
    HTTP-01 against the ingress's port.
- **Existing tests that assert the seeded list is exactly `["autoscaler"]`**
  are updated to include the ingress.

## 10. Rollout

- **Every cluster gets the record** on its next leader pass, seeded
  disabled, so nothing binds a port until someone runs `legion app enable
  ingress` (or seeds it on).
- **The `ingress` key is registered everywhere.** A job that sets it on a
  cluster with the app disabled is accepted with a warning.
- **The app is bundled, so it upgrades with the binary.** An upgrade is a
  rolling restart of the app's job. Static ports stop the old replica
  before the new one starts on the same node, so expect a brief gap per
  node. Run two or more replicas behind DNS to cover it. The docs state
  this.
- **Nothing existing changes behaviour.** Exec hardening is additive, and
  the autoscaler is still seeded enabled.

## 11. Open questions

- **Should the ingress be on by default?** Lean: no (calls entry 5).
  Revisit if a manager-config "edge" flag appears.
- **Should routes also be app objects** (`ingress/Route`), for hosts that
  point outside Legion? Lean: later, if asked. A service is the common
  target.

## 12. Acceptance criteria

**Phase 1**
- `legion app enable ingress` on a fresh cluster starts the app on a
  manager, listening on 80, as its uid, with only `CAP_NET_BIND_SERVICE`.
- A job with `service.ingress.host = "a.test"` is reachable with
  `curl -H 'Host: a.test' http://<node>/`, its requests carry
  `X-Forwarded-For`, and a second job claiming `a.test` is refused at
  submit.
- Stopping one of two backends moves all traffic to the other within one
  refresh, with no client errors for GETs.

**Phase 2**
- ~~With `acme.directory` pointed at Pebble, a routed host gets a
  certificate within one issuer pass.~~ Revised: a routed host gets a
  Certificate in `legion-system`, the cert manager issues it through
  HTTP-01 answered by the ingress with no failed order, every replica
  serves it, and `http://` redirects.
- `legion service resolve legion-system/ingress` and the Ingress page name
  the addresses to point DNS at, and each route's DNS is checked.
- An operator bundle under `legion/apps/ingress/certs/` is served for its
  names, and no order is placed for them.
