# Ingress

The ingress puts your services on the internet. Add one line to a service,
point DNS where the ingress tells you, and the service answers at its
hostname over HTTPS, with a certificate Legion issues and renews for you.

```nickel
service = {
  port = "http",
  ingress = { host = "shop.example.com" },
},
```

The ingress is one of Legion's [bundled apps](apps.md), like the
autoscaler. It is a proxy on ports 80 and 443 of the nodes it runs on. It
routes each request by hostname and path to the healthy instances of the
service that claimed them, and it gets a certificate for each hostname from
the bundled [cert manager](cert-manager.md), from Let's Encrypt by default.
You don't install a proxy, write proxy config, or set up certificates.

## Quick start

**1. Turn it on.** The ingress is off on a new cluster, because it takes
ports 80 and 443 on the nodes it runs on.

```console
$ legion app enable ingress
$ legion app status ingress
NAME     VERSION  SOURCE   STATUS   PLACEMENT  REPLICAS  UI        DETAIL
ingress  0.1.0    bundled  running  managers   1/1       nav, job  on legion-system-x8f2k1qz
```

It runs on a manager by default, listening on ports 80 and 443.

**2. Route a service.** Add `ingress` to the service block and run the
job:

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

```console
$ legion job run shop.ncl
```

**3. Point DNS at it.** Ask where:

```console
$ legion service resolve legion-system/ingress
ADDRESS        PUBLIC          ALLOC  NODE                    HEALTHY  STATUS
10.0.3.17:80   203.0.113.7:80  12     legion-system-x8f2k1qz  yes      running
```

Create an `A` record for `shop.example.com` pointing at the `PUBLIC`
address, or at `ADDRESS` when `PUBLIC` is `-`. The Ingress page in the UI
(`legion app view ingress nav` in the terminal) says the same under **Point
DNS here**, and its Routes table checks each host's DNS for you
([Where to point DNS](#where-to-point-dns)).

Once DNS points at the ingress, the cert manager gets a certificate from
Let's Encrypt (the CA checks the name over HTTP-01, which the ingress
answers), and within a minute or so:

```console
$ curl -I http://shop.example.com/
HTTP/1.1 308 Permanent Redirect
location: https://shop.example.com/

$ curl -s https://shop.example.com/ | head -1
<!doctype html>
```

Until DNS points at the ingress, you can try a route with an explicit
`Host` header against any ingress node:

```console
$ curl -H 'Host: shop.example.com' http://10.0.3.17/
```

## Routing a service: the `ingress` key

`ingress` goes on a task's `service` block. It is a
[jobspec extension](apps.md#jobspec-extensions) the ingress app declares,
so it is checked at submit and listed by `legion app extensions ingress`
and `legion job schema`. `apps.ingress` is the same key in its qualified
spelling.

| Field | Default | Meaning |
| --- | --- | --- |
| `host` | required | The hostname to route, or a list of up to 16. `*.example.com` matches any single label under `example.com`. Case-insensitive. |
| `path` | `"/"` | Route only requests under this path prefix. It matches on segment boundaries: `/api` matches `/api` and `/api/users`, not `/apix`. |
| `stripPath` | `false` | Remove `path` before the request reaches the service: with `path = "/api"`, `/api/users` arrives as `/users`. |

The route goes to the healthy endpoints of the service it is written on:
that job's task's service, on the port the service advertises.

```nickel
# The apex and www, together.
ingress = { host = ["example.com", "www.example.com"] },

# An API under /api on the same host as a site. The longer prefix wins:
# /api/... goes here, everything else to the site.
ingress = { host = "example.com", path = "/api", stripPath = true },

# Every tenant subdomain to one service (HTTPS needs a DNS-01 Issuer or
# your own wildcard certificate; see below).
ingress = { host = "*.apps.example.com" },
```

Changing only the `ingress` value and resubmitting restarts nothing. The
job's allocations stay where they are, and the ingress follows the new
value within a second or two
([Changing only extension values restarts nothing](apps.md#changing-only-extension-values-restarts-nothing)).

### Checks at submit

The schema is enforced at submit, whether or not the app is running:

```text
Error parsing jobspec: shop: task web: service: ingress.host: "not a host" is not valid under any of the schemas listed in the 'oneOf' keyword
Error parsing jobspec: shop: task web: service: ingress.path: "api" does not match "^/[^?#\s]*$"
Error parsing jobspec: shop: task web: service: ingress: Additional properties are not allowed ('tls' was unexpected)
```

While the ingress runs, it is also asked about each value. It refuses a
host and path another job's service already serves:

```text
shop: task web: service web: ingress (ingress app): shop.example.com/ is already routed to storefront (task web, service web)
Hint: the app refused these values; fix them and submit again
```

It also warns about names its Issuer cannot prove:

```text
warning: shop: task web: service web: ingress (ingress app): shop.internal gets no automatic certificate (no public CA issues for shop.internal: serve it over HTTP, or use a ca Issuer); it is served over HTTP unless an operator bundle under legion/apps/ingress/certs/ covers it
```

While the app is disabled, values are accepted after the schema check, with
a warning that says so:

```text
warning: shop: task web: service web: ingress was not checked by the ingress app (disabled); the schema check passed
```

### Which route answers

For a request to host `h` and path `p`:

1. The routes for `h` exactly, longest matching `path` first.
2. If none matches, the routes for `*.parent` of `h` (one label only:
   `*.example.com` matches `a.example.com`, not `a.b.example.com`), longest
   `path` first.
3. Otherwise the ingress answers `404 no_route`.

**Two services claiming the same host and path.** The ingress refuses the
second submit, as above. If two claims still meet, the lower `(job, task,
service)` serves it on every replica, and the other shows up on the Ingress
page with the reason. That can happen when the app was down at the
submit, or when two submits raced. A stopped job keeps its values but
claims nothing until it runs again.

## Where it runs

The ingress app's job binds static ports 80 and 443. At most one replica
can hold them on a node, so replicas always land on different nodes:

```console
$ legion app set ingress --replicas 3          # three managers, one each
$ legion app set ingress --placement workers --pool edge --replicas 4
```

| Setting | Default | |
| --- | --- | --- |
| placement | `managers` | The managers' [system workers](architecture-managers-workers.md#the-system-worker). Right for clusters whose managers face the internet. |
| replicas | `1` | One per node. Point DNS at every node that runs one (round-robin `A` records, or a cloud load balancer across them). |
| resources | 500 MHz, 256 MiB | `legion app set ingress --cpu 2000 --memory 512`. On managers, it shares the system worker's lend (`systemWorker.resources`, 1000 MHz and 1024 MiB by default) with the other bundled apps. |

`--placement workers` (or `any`) needs `operator:write`
([Placement](apps.md#placement)). The workers it lands on must accept the
`legion-system` namespace, so give the edge worker type
`namespaces = ["*", "legion-*"]` or `["legion-system"]`
([Nodes that accept only some namespaces](namespaces.md#nodes-that-accept-only-some-namespaces)).

**Sizing.** On managers, the system worker's lend is a hard ceiling: the
cgroup caps the ingress, the cert manager, and the other bundled apps
together at `systemWorker.resources`, one CPU by default. That is plenty
for a small site and for TLS handshakes at a modest rate, but a busy site
will hit it, and the proxy then slows rather than fails. For real traffic,
run it on edge workers (`--placement workers --pool edge`) with
`--cpu`/`--memory` to match, or raise `systemWorker.resources` on the
managers.

To seed it on for every new cluster, set it in the manager config:

```nickel
# configs/manager.ncl
apps = { ingress = { enabled = true } },
```

### Where to point DNS

The ingress's job declares a catalog service, `legion-system/ingress`, so
its replicas are listed like any service's endpoints:

```console
$ legion service resolve legion-system/ingress -j | jq -r '.data[] | .publicAddress // .address'
203.0.113.7
10.0.3.18
```

Point each routed host at those addresses: one `A` (or `AAAA`) record per
address, or a cloud load balancer across them. Listing `legion-system`
takes a rule that names it (an operator's token has one); the Ingress page
shows the same addresses to anyone who can open it. `PUBLIC` is the address the
node's worker reports with `LEGION_WORKER_PUBLIC_ADDRESS`; a node without
one shows only its cluster address. On a cloud VM behind NAT, start its
worker with `LEGION_WORKER_PUBLIC_ADDRESS=<public IP>` and the public
address appears here and on the page.

The Ingress page's **Point DNS here** block lists the same addresses, and
its Routes table has a **DNS** column. The ingress resolves every routed
host from its own node, at once for a new route and every minute after:

| DNS column | Meaning |
| --- | --- |
| `points here` | The host resolves to at least one replica's address (cluster or public). |
| `points to 198.51.100.4` | It resolves, but to none of them: an old record, or another server. |
| `no record` | It does not resolve. |
| `-` | A wildcard: there is no single name to look up. |

The check sees DNS as the ingress's node does, so split-horizon DNS, or a
CDN or load balancer in front of the ingress, reads `points to` while the
route works fine.

### Ports below 1024 without root

Apps run as their own unprivileged uid
([Trust model](apps.md#trust-model)). The ingress's task keeps one
capability, `CAP_NET_BIND_SERVICE`, through
`hardening.capabilities.add = ["net_bind_service"]`
([Process hardening](task-supervision.md#process-hardening)). That lets it bind
80 and 443 itself, so connections reach it with the client's real address,
and nothing sits between the client and the proxy.

## HTTPS

Port 443 serves a certificate chosen by the name the client asks for (SNI),
and offers HTTP/2 and HTTP/1.1. A name with no certificate fails the
handshake rather than serve the wrong one.

### Automatic certificates

The ingress does not order certificates itself: the bundled
[cert manager](cert-manager.md) does. For every routed host its Issuer can
prove, the ingress keeps one `certs/Certificate` in `legion-system` and
serves what the cert manager writes for it. You see them like any other
Certificate:

```console
$ legion object list certificates -n legion-system
```

- **The Issuer.** By default it is `ingress`, an ACME Issuer for Let's
  Encrypt over HTTP-01 that the ingress creates and keeps from its `acme`
  config. It serves only `legion-system`, so tenants cannot use it. Name
  another Issuer in the config to use that one instead
  ([Using your own Issuer](#using-your-own-issuer)).
- **The ingress answers HTTP-01.** On port 80,
  `/.well-known/acme-challenge/<token>` gets `<token>.<thumbprint>`, where
  the thumbprint is the Issuer's public ACME account thumbprint. Every
  replica answers every challenge, with no shared state and no list of
  pending challenges. Challenges are never redirected to HTTPS.
- **Nothing is asked for before the account exists.** Until the cert
  manager has registered the Issuer's ACME account, the ingress could not
  answer a challenge, and a failed first order would wait out the cert
  manager's 5-minute backoff. So new Certificates wait for the account, and
  the page reads `waiting for the Issuer to register its ACME account`.
- **One Certificate per host.** Each is named after its host (`shop.example.com`
  becomes `shop-example-com-<hash>`), labeled `ingress/managed=true` and
  `ingress/host=<host>`, and written to `legion/apps/ingress/issued/<name>`,
  the ingress's own KV. When a route goes, its Certificate and its key go
  with it.
- **Renewal, retries, and rate limits** are the cert manager's
  ([When it is issued](cert-manager.md#when-it-is-issued),
  [Failures and rate limits](cert-manager.md#failures-and-rate-limits)):
  renewal when a third of the lifetime is left, and failures backing off
  from 5 minutes to 6 hours. The usual cause of a failure is DNS that does
  not point at the ingress yet.
- **One writer.** Every replica follows the Issuer and the Certificates,
  but only the holder of a KV lock writes them, so two replicas that see a
  route change a moment apart do not undo each other. Every replica loads
  new certificates within 10 seconds.
- **Names it does not ask for**, with an HTTP-01 Issuer: wildcards (HTTP-01
  cannot prove them), IP addresses, single-label names, and the names
  reserved for local and test use: `localhost`, `.local`, `.internal`,
  `.test`, `.example`, `.invalid`, `.lan`, `.localdomain`, `.home.arpa`.
  Those are served over HTTP, or over HTTPS with your own Issuer or bundle.
  The page says why for each.

**Turning the ingress on agrees to the CA's terms of service** (Let's
Encrypt's subscriber agreement, by default) on your behalf, as Caddy does:
the default Issuer registers an account. Set `certificates = false` if you
will not.

Point the default Issuer at Let's Encrypt's staging environment while you
test, or at another ACME CA, through the app's config:

```console
$ cat ingress.json
{ "acme": { "server": "letsencrypt-staging", "email": "ops@example.com" } }
$ legion app config set ingress -f ingress.json
```

| Field | Default | Meaning |
| --- | --- | --- |
| `certificates` | `true` | Ask the cert manager for a certificate per routed host. Turn it off behind a load balancer that terminates TLS. |
| `issuer` | `ingress` | The `certs/Issuer` that issues them. `ingress` is the one the app keeps from `acme`; any other name must already exist. |
| `acme.server` | `letsencrypt` | The default Issuer's CA: `letsencrypt`, `letsencrypt-staging`, or an ACME directory URL (`https://`). Ignored when `issuer` names another. |
| `acme.email` | none | The default Issuer's account contact address. |
| `redirectHttps` | `true` | Redirect plain HTTP to HTTPS for hosts with a certificate ([Redirects](#redirects)). |

`issuer` and `acme.server` are [code fields](apps.md#config): changing them
needs the app's whole grant, because they decide who can obtain
certificates for your hosts.

The cert manager is on by default on every cluster. If it is disabled,
nothing is issued: the page's Certificates notice says the Issuer is not
ready (or missing), and hosts are served over HTTP, or from your own
bundles, until it is back (`legion app enable certs`).

### Using your own Issuer

The default Issuer proves exact public names over HTTP-01. For anything
else, write an Issuer that can, list `legion-system` in its `namespaces`
(`*` does not reach the reserved `legion-*` namespaces), and name it in the
ingress's config:

- **Wildcard hosts** (`*.apps.example.com`): an ACME Issuer with `dns01`
  ([DNS-01](cert-manager.md#dns-01)).
- **Internal names** (`admin.internal`): a private CA
  ([A private CA](cert-manager.md#a-private-ca)). Every name is provable,
  and clients must trust the CA.
- **A CA that needs an account binding** (ZeroSSL, Google): ACME with
  `eab` ([ACME](cert-manager.md#acme)).

```nickel
# edge-issuer.ncl
{
  kind = "certs/Issuer",
  name = "edge",
  spec = {
    namespaces = ["legion-system"],
    acme = {
      email = "ops@example.com",
      dns01 = { cloudflare = { apiToken = { kv = "secrets/cloudflare-dns" } } },
    },
  },
}
```

```console
$ legion object apply -f edge-issuer.ncl --wait
$ echo '{ "issuer": "edge" }' > ingress.json
$ legion app config set ingress -f ingress.json
```

The ingress then asks `edge` for every routed host it can prove, wildcards
included, and stops keeping its own `ingress` Issuer (it does not delete
it). An Issuer that does not serve `legion-system` is named on the page,
with the fix.

### Your own certificates

Write a PEM bundle (the certificate chain, leaf first, then the private key)
anywhere under `legion/apps/ingress/certs/`:

```console
$ legion kv put legion/apps/ingress/certs/corp-wildcard "$(cat wildcard.crt chain.crt wildcard.key)"
```

The ingress serves it for every DNS name in the leaf's subject alternative
names, wildcards included, within 10 seconds, and asks the cert manager for
nothing it covers. Your bundle wins over an issued certificate for the same
name.
Replace the key to rotate it, and delete it to stop serving it. A bundle
that does not load (a key that does not match its certificate, or a leaf
with no DNS names) is listed on the Ingress page with the reason.

`legion/` is a [reserved prefix](namespaces.md#reserved-namespaces): writing
there takes a rule that names it, such as
`{ resources = ["kv"], actions = ["write"], paths = ["legion/apps/ingress/certs/*"] }`.
A tenant's `paths = ["*"]` does not reach it.

### Redirects

Plain HTTP to a host that has a certificate gets a `308` to the same URL on
`https://`. Hosts without one are served over plain HTTP. ACME challenges
are never redirected. Set `redirectHttps = false` in the config to serve
both.

## What a request goes through

- **Clients** speak HTTP/1.1, or HTTP/2: by ALPN on 443, and by prior
  knowledge on 80.
- **Services** get HTTP/1.1, on connections the ingress keeps open and
  reuses: up to 32 idle per endpoint, each for at most 50 seconds.
- **Headers.**
  - `Host` is kept as the client sent it.
  - `X-Forwarded-For` gets the client's address appended. A client can send
    its own `X-Forwarded-For`, so trust only the last entry.
  - `X-Forwarded-Proto` is `http` or `https`.
  - `X-Forwarded-Host` is the host.
  - Hop-by-hop headers (`Connection` and what it names, `Keep-Alive`, `TE`,
    `Trailer`, `Transfer-Encoding`, `Proxy-*`) are not forwarded.
- **Bodies stream** both ways. Nothing is buffered.
- **Balancing** is power of two choices on in-flight requests: of two
  endpoints, the less busy gets the request.
- **Retries.** An endpoint that refuses a connection is skipped for 10
  seconds, and the request goes to another, up to 3 endpoints. The ingress
  retries only requests that never reached a service, so a `POST` is never
  sent twice.
- **WebSockets** and other HTTP/1.1 upgrades are relayed: after the
  service's `101`, bytes are copied both ways until either side closes.
- **Endpoints** come from the [service catalog](service-discovery.md): the
  healthy instances of the task the route is written on, re-read every 2
  seconds and whenever routes change. A new instance gets traffic once it
  passes its checks. If the manager is unreachable, the ingress keeps
  routing to the endpoints it last saw.
- **Timeouts.**
  - Connecting to an endpoint: 3 seconds.
  - A service's response headers: 60 seconds. A response that is streaming
    is never cut off.
  - A client's request head: 30 seconds.

## Errors

The ingress's own errors use the standard envelope
([agent readiness](agent-readiness.md)) and a `Legion-Ingress-Error`
header:

| Status | Code | When |
| --- | --- | --- |
| `404` | `no_route` | No route for the host and path. |
| `503` | `no_endpoint` | The route's service has no healthy endpoint. `Retry-After: 1`. |
| `502` | `bad_gateway` | No endpoint accepted a connection, or the service broke the connection mid-response. |
| `504` | `upstream_timeout` | The service sent no response headers within 60 seconds. |
| `400` | `bad_request` | The request names no host. |

```json
{"ok":false,"error":{"code":"no_endpoint","message":"shop (task web, service web) has no healthy endpoint","hint":"check `legion service resolve` and the job's health checks","retryable":true}}
```

## Seeing it

- **The Ingress page** (sidebar; `legion app view ingress nav`). It shows:
  - **Point DNS here**: every replica's node and address;
  - every route you can read, with its URL, its service's healthy and total
    endpoints, its DNS, its certificate's state, and any problem (a lost
    conflict, a stopped job, no healthy endpoint, a mesh-only service);
  - every certificate, with its source (your bundle or the cert manager),
    issuer, and expiry; Certificates the cert manager has not issued, with
    its reason; hosts it was not asked for, with why; and bundles that do
    not load;
  - a notice when the Issuer is missing, not ready, or does not serve
    `legion-system`.

  Only namespaces where you hold `job:read` are listed.
- **The Ingress panel on a job's page** (`legion app view ingress job
  shop`): that job's routes and their URLs.
- **`legion app status ingress`** and `/healthz` on the app's `api` port
  report route and certificate counts. The app is unhealthy only when it
  cannot bind its ports. A lost connection to the manager is reported, and
  routing carries on.
- **Logs** (`legion app logs ingress -f`): routes changing, Certificates
  asked for and dropped, bundles loaded, the certificates lock moving
  between replicas. The cert manager logs the orders themselves
  (`legion app logs certs -f`).

## Behind a load balancer

If a cloud load balancer terminates TLS in front of the ingress, turn
certificates off. Requests then arrive on port 80 and are routed by `Host`
as usual:

```json
{ "certificates": false }
```

The load balancer must pass `Host` through. The ingress appends the load
balancer's address to `X-Forwarded-For`, after the client's.

## Security

- **What the app may do.** The ingress's grant is exactly what it calls:
  - `job:read` in every tenant namespace, to read the service catalog, and
    on its own job in `legion-system`, to list its replicas;
  - `kv` read and write under its own `legion/apps/ingress/`;
  - `certs/Certificate` read and write in `legion-system`, `certs/Issuer`
    read, and `certs/Issuer` write on the one named `ingress`.

  It cannot change jobs, read anyone else's KV, or touch another Issuer or
  another namespace's Certificates. It runs as its own uid, holding only
  `CAP_NET_BIND_SERVICE`.
- **Where its secrets live.** Certificate private keys live in KV under
  `legion/apps/ingress/`: your bundles under `certs/`, and what the cert
  manager issues under `issued/`. The cert manager writes there only acting
  for one of the ingress's own Certificates, which only the ingress can
  write ([Access](cert-manager.md#access)). The ACME account key is the
  cert manager's, under `legion/apps/certs/`. That KV is encrypted at rest
  when [KV encryption](kv-encryption.md) is on. Reading it takes a rule that
  names the reserved prefix, or a management token.
- **Hostnames are first come, first served.** Any job author whose service
  can be routed can claim any hostname nobody has claimed yet. On a cluster
  whose DNS points a wildcard at the ingress, that author can then get a
  certificate for the name. Legion does not yet restrict which namespace may
  claim which domain. On a cluster shared by teams that do not trust each
  other, point DNS only at the names you mean to serve.
- **`network.ingress` is something else.** A task's
  [`network.ingress`](network-handlers.md#ingress) is the datapath's policy on
  who may connect to its ports. It also applies to the ingress's
  connections: a chain that denies the ingress's node addresses denies its
  traffic too.

## Limitations

- **HTTP/1.1 to services.** A service that only speaks HTTP/2 (gRPC) cannot
  be routed.
- **No routes to `mesh = true` services.** Their ports take only the
  mesh's node-to-node mTLS, which the ingress does not hold. The Ingress page
  names the cause. Advertise a second, non-mesh service for the ingress.
- **No TCP or TLS passthrough.** Only HTTP is routed.
- **Endpoints are polled**, every 2 seconds per routed service, not
  watched. Combined with the catalog's own heartbeat delay, a new instance
  can take a few seconds to receive traffic.
- **Upgrades cause a short gap per node.** Upgrading Legion rolls the
  ingress's job. A static port is released before the new replica binds it,
  so each node stops answering for a few seconds. Run two or more replicas
  behind DNS to ride through it.
- **IPv4 listeners.** The ingress listens on `0.0.0.0`.
- **Wildcards and internal names need an Issuer of your own** (DNS-01 or a
  private CA), or your own bundle. The default Issuer proves exact public
  names over HTTP-01 only.
- **A brand-new Issuer with several replicas.** Each replica reads the
  Issuer's thumbprint every 5 seconds until it has it, and the writer asks
  for Certificates as soon as it does. For those few seconds, a challenge
  that reaches another replica gets `503`, and the cert manager retries
  after its backoff. Once the account exists the thumbprint never changes.
- **The DNS check is from the ingress's node**, and only for exact names.
- **No metrics endpoint or access log yet.** Routes and certificates are
  on the Ingress page, and the app logs routes and Certificates.
- **No waking a suspended or scaled-to-zero service.** To wake suspended
  keyed instances on a request, use the [gateway](gateway.md).

## Reference

- The manifest: `src/apps/bundled/ingress.ncl` (`legion app config schema
  ingress` prints the config schema).
- The key: `legion app extensions ingress`, `legion job schema`.
- Bundled apps, settings, and the trust model: [apps.md](apps.md).
- The cert manager, Issuers, and Certificates: [cert-manager.md](cert-manager.md).
- Exec capabilities: [task-runtime-exec.md](task-runtime-exec.md),
  [task-supervision.md](task-supervision.md#process-hardening).
