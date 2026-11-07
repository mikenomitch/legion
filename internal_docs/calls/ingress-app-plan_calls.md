# Judgement calls: Ingress

Calls made while building [ingress-app-plan.md](../plans/ingress-app-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. The app binds 80 and 443 itself, with an ambient `CAP_NET_BIND_SERVICE`, not behind the datapath's root listener

Bundled apps run as their own unprivileged uid, so the ingress cannot bind a
port below 1024 on Linux. The exec runtime gains one capability it can
*add*, and the manifest asks for it:

```nickel
config = {
  command = ["legion:self", "app", "run", "ingress"],
  hardening = { capabilities = { add = ["net_bind_service"] } },
},
```

```text
Error: exec hardening: capabilities.add may name only net_bind_service, which lets a task bind ports below 1024 without root; "sys_admin" would make it root-equivalent
```

- **Alternatives.**
  - (a) Declare an ingress chain on the ports (`network.ingress = { policy
    = "allow" }`). The worker's datapath, which is root, then binds 80 and
    443 and splices to a private twin port the app binds. That needs no new
    mechanism.
  - (b) Socket activation: the worker binds the listener and passes the
    descriptor to the task (`LISTEN_FDS`). This is zero-cost, and it keeps
    the socket open across app restarts.
  - (c) Default to unprivileged ports (8080/8443) and tell operators to put
    a load balancer or iptables redirect in front.
- **Trade-off.** It is a new knob in a security-sensitive path, and
  Linux-only in effect. A task that holds it can take any free port below
  1024 on its node, including on a manager. That port could be one a
  service expected to bind later, such as 53, 25, or 22 while sshd is
  down. The knob is allowed on any exec task, not just apps. Outside
  `legion-system` a job author can already run exec as root, so the knob
  grants no one anything they could not already get.
- **Why (performance, simplicity).**
  - (a) is disqualifying for an ingress. The datapath does not pass the
    client's address to the task (`docs/network-handlers.md`, "Ingress":
    no PROXY header, no forwarding headers), so `X-Forwarded-For` would
    name the node. It also adds a loopback hop: about 45-80 µs per new
    connection by `net_bench`, and every internet client connection is
    new.
  - (b) is the better mechanism in the long run. But it reaches into
    spawn, restart, and the re-adoption of running tasks after a worker
    restart, which is a plan of its own.
  - (c) fails "incredibly easy".
  - Only `net_bind_service` may be added. Every other capability is a path
    to root, and the pre-exec code sets the permitted set to exactly that
    bit before `exec`, failing the start on any error.

## 2. Routes live on the service as an app jobspec key, `service.ingress`, not as a route object or tags

```nickel
service = { port = "http", ingress = { host = "shop.example.com" } },
```

- **Alternatives.**
  - (a) An app object kind, `ingress/Route { host, path, service }`, applied
    with `legion object apply`: the Kubernetes Ingress/HTTPRoute shape.
  - (b) Free-form tags or `meta` on the service (`meta.ingress.host`): the
    Traefik-on-Consul shape.
  - (c) Routes in the app's config, edited by the operator.
- **Trade-off.** A route cannot point outside Legion, or at a service the
  author does not own. Changing a hostname is a job submit, though it is
  an in-place one: extension-only changes restart nothing. A host shared by
  two services under different paths is declared twice, once on each.
- **Why (simplicity).**
  - The route is part of the service's definition and is deployed,
    reviewed, and destroyed with it. A job destroyed is a route gone, with
    no orphaned objects.
  - The extension system validates the value at submit with the app's
    schema and its validate call, so a typo is an error, not a silent 404
    (the failure mode of (b)).
  - (a) doubles what a developer writes and adds a second thing to keep in
    sync. (c) puts the operator in every deploy's path.
  - An object kind stays possible for routes to outside targets (plan
    §11).

## 3. The ingress is seeded disabled; `legion app enable ingress` turns it on

`BundledApp` gains `seed_enabled`. The autoscaler keeps `true` and the
ingress takes `false`. The manager config's `apps.ingress.enabled = true`
seeds it on.

- **Alternatives.** Seed it enabled like the autoscaler, so a route works
  the moment a job declares one. Or start it on demand when the first
  `ingress` value is submitted.
- **Trade-off.** It costs one command before the first route works, and a
  job that sets `ingress` on a cluster where the app is off is accepted
  with a warning rather than served.
- **Why (simplicity, safety).** Ports 80 and 443 on every manager of every
  new cluster are not Legion's to take by surprise. A dev laptop, a manager
  that already runs a web server, and a CI box would each get an app stuck
  restarting on a port conflict. The validate call's "not checked: the app
  is disabled" warning is the nudge, and the public doc's first command is
  the enable. On-demand start was rejected because a submit starting a
  privileged listener on the managers is a side effect no reviewer of that
  submit would expect.

## 4. Automatic HTTPS is on by default, and the app agrees to the CA's terms on the operator's behalf

With the app enabled, every routed public hostname gets a Let's Encrypt
certificate with no configuration, and HTTP redirects to HTTPS once one is
served. `acme.enabled = false` turns ordering off. That suits a cluster
behind a TLS-terminating load balancer.

- **Alternatives.** ACME off until the operator sets `acme.email` and
  `acme.agreeToTerms = true`, the cert-manager and Traefik shape.
- **Trade-off.** Enabling the app is agreeing to the CA's subscriber
  agreement, which the operator may not have read. A host whose DNS does
  not point at the ingress spends failed validations, bounded by the per-host
  backoff (5 minutes, doubling to 6 hours), which stays inside Let's
  Encrypt's limit of 5 failures per host per hour.
- **Why (simplicity).** Caddy made this the expectation, and it is what
  "incredibly easy" means for an ingress: a route is HTTPS. The public doc
  states the agreement in its first section. Names no public CA can issue
  for (`localhost`, `.internal`, `.test`, IP literals, single labels) are
  never ordered, so a dev cluster does not hammer the CA.

## 5. A small RFC 8555 client in the tree instead of `instant-acme`

`src/apps/ingress/acme.rs` implements the five ACME exchanges Legion needs,
on crates already in the build: `ring` ES256 JWS, the RFC 7638 thumbprint,
`rcgen` CSRs, and async `reqwest`. Those exchanges are new account, new
order, the HTTP-01 challenge, finalize, and download.

- **Alternatives.** `instant-acme` 0.8 with its `ring` feature and its own
  HTTP client switched off. It is well maintained and has seen far more
  CAs.
- **Trade-off.** Legion owns the protocol code. Edge cases a library has
  already met are ours to meet: a CA's quirky `Retry-After`, an
  `orderNotReady` race, ARI. It is tested against Pebble, Let's Encrypt's
  own test CA, but only when a Pebble binary is present, which CI does not
  have.
- **Why (performance of the build, simplicity of the tree).** The library
  would pull a second `rcgen` and a second `x509-parser` major unless its
  features are trimmed. With them trimmed, it still needs a hand-written
  HTTP client adapter, which is a third of the work of the client itself.
  The tree's pattern is to avoid a dependency when what it does is small
  and specified. Here it is about 400 lines, and RFC 8555 is stable.

## 6. One issuer per cluster, chosen by a KV lock; every replica serves from KV

The holder of `legion/apps/ingress/acme/lock` (60 s lease, renewed every
20 s) is the only replica that orders certificates. Bundles, the account,
and HTTP-01 answers live under `legion/apps/ingress/`. A replica that the
CA's validation request reaches looks the token up in KV.

- **Alternatives.** Every replica orders for itself: Caddy's default,
  without shared storage. Or answer challenges only on the issuing replica
  and hope DNS sends the CA there.
- **Trade-off.** A challenge request costs a KV read on a replica that did
  not issue it. Unknown tokens are bounded by a format check and a small
  concurrency cap. Certificates reach other replicas on their next 10-second
  KV poll, not instantly. The private keys sit in KV, readable by whoever
  holds `kv:read` on `legion/apps/ingress/*`: management tokens and rules
  that name the reserved prefix.
- **Why (correctness, simplicity).** N replicas ordering independently
  multiply load on the CA's rate limits by N, and their challenges race
  each other behind round-robin DNS. One issuer plus shared answers is
  correct for any number of replicas behind any DNS. KV already exists and
  is encrypted at rest when the cluster turns that on, so there is no new
  store.

## 7. Endpoints are polled every 2 seconds, not watched

Each routed service is resolved with `ResolveService` every 2 seconds, at
most 16 at a time.

- **Alternatives.** Add a `WatchServices` server stream to the manager
  first (TODO: "A watchable catalog for outside load balancers"). It would
  carry endpoint and health changes as they land.
- **Trade-off.** A new healthy endpoint takes up to 2 seconds plus a
  heartbeat to receive traffic. A removed one can keep receiving requests
  for the same window. Passive ejection covers the dead-endpoint half: a
  dial failure ejects an endpoint at once and retries elsewhere. The
  manager answers routes × replicas / 2 small indexed queries per second.
- **Why (simplicity now, performance later).** The watch is a manager
  feature with its own design: what a health change is, and how to fan it
  out. It deserves its own change, and it would also serve Envoy and
  external-dns. The ingress hides the transport behind one function, so
  switching it over is local. This is Phase 3.

## 8. A per-request HTTP proxy, not the gateway's read-one-head-and-splice

- **Alternatives.** Reuse `src/gateway.rs`'s model: read the first head,
  pick a backend, splice the connection.
- **Trade-off.** Every request is parsed and re-serialized, which costs CPU
  that splicing avoids for large bodies. Bodies still stream, and nothing
  is buffered.
- **Why (correctness, performance).** Browsers reuse a connection across
  paths, so routing `/api` and `/` to different services, and balancing
  each request across replicas, needs a decision per request. A spliced
  keep-alive connection would pin every later request to the first
  backend. A per-request proxy also lets clients speak HTTP/2 to the edge
  while backends keep HTTP/1.1, and backend connections are pooled.

## 9. Backends get HTTP/1.1 only

- **Alternatives.** Speak h2c to backends that declare
  `meta.protocol = "h2c"` or `"grpc"`.
- **Trade-off.** gRPC services cannot be routed in this release, and a
  backend that would prefer HTTP/2 gets one request per connection at a
  time, pooled.
- **Why (simplicity).** HTTP/1.1 is what every web framework serves by
  default, and the pool and upgrade paths are simpler with one protocol.
  This is in Phase 3.

## 10. The first claim on a host and path wins; a conflicting submit is refused

The key sets `validate = true`. The running app refuses a value whose
`(host, path)` another service holds. If two values still conflict (the app
was down at submit, or two submits raced), every replica picks the same
winner: the lower `(job, task, service)`. The loser is shown in the view.

- **Alternatives.** Newest wins (a redeploy can take a host over). Or
  per-namespace domain ownership in the app config.
- **Trade-off.** In a multi-tenant cluster, any namespace can claim any
  hostname nobody has claimed yet, and then the ingress orders a
  certificate for it. The public doc says this plainly. The fix is the
  Phase 3 `domains` rule. The tie-break by name is arbitrary, but it is
  stable.
- **Why (safety, simplicity).** "Newest wins" lets any deploy silently
  steal production traffic. Ownership rules are the right tool, but they
  are a policy surface of their own. First-claim-wins is the one rule that
  cannot take a working site away.

## 11. The key is called `ingress`, beside the unrelated `network.ingress` chain

- **Alternatives.** `expose`, `public`, `route`, `http`, `domain`.
- **Trade-off.** One jobspec now holds `network.ingress`, the datapath
  policy on who may connect in, and `service.ingress`, the public route to
  the service. They mean different things.
- **Why (simplicity).** An app's key defaults to its name, so `ingress`
  lines up with `legion app … ingress`, `apps.ingress`, the docs page, and
  what every operator calls this. The two sit on different blocks, and the
  docs cross-reference them.

## 12. `host` takes a string or a list

```nickel
ingress = { host = "shop.example.com" }
ingress = { host = ["example.com", "www.example.com"] }
```

- **Alternatives.** Always a list (`hosts = [...]`), or one `host` plus
  `aliases`.
- **Trade-off.** One field has two shapes, which the schema expresses as
  `oneOf`.
- **Why (simplicity).** One name is the common case and reads naturally.
  The apex-plus-`www` pair is the next most common, and an app key gets
  one record per service, so it has to fit in the same field.
  `service.port` already takes a label, a number, or `"dynamic"`.

## 13. One certificate per hostname

- **Alternatives.** One certificate per route, carrying all of its hosts
  as SANs.
- **Trade-off.** More orders: `example.com` and `www.example.com` are two.
  Let's Encrypt allows 50 per registered domain per week, which is ample
  for this.
- **Why (simplicity, robustness).** A host that fails validation, because
  its DNS is not pointed yet, does not block the others. Renewals and
  removals are per name. Nothing has to be re-ordered when a route gains or
  loses a host.

## 14. Routes to `mesh = true` services are reported, not served

- **Alternatives.** Give the app a node certificate so it can originate
  mesh mTLS with a PROXY v2 TLV, as the datapath does.
- **Trade-off.** A mesh service needs a second, non-mesh service on another
  port to be routed.
- **Why (security).** The node certificate is the node's identity. Handing
  it to an app process would let that app speak as the node to every mesh
  service. The view and the route's problems name the cause.
