# Judgement calls: Ingress

Calls made while building [ingress-app-plan.md](../plans/ingress-app-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. Certificates come from the cert manager: the ingress keeps one `certs/Certificate` per routed host and answers HTTP-01, and runs no ACME client of its own

The first build of Phase 2 had its own RFC 8555 client, account, challenge
board, and issuer lock (entries 19 and 20). The bundled cert manager landed
beside it with an Issuer/Certificate model, and the two were doing the same
work. Now, for every routed host its Issuer can prove, the ingress keeps a
Certificate in `legion-system` that writes under its own KV prefix, and
serves what the cert manager writes there:

```console
$ legion object list certificates -n legion-system
$ legion app config set ingress -f ingress.json   # { "issuer": "edge" }
```

```json
{ "certificates": true, "issuer": "ingress",
  "acme": { "server": "letsencrypt", "email": "ops@example.com" } }
```

- **Alternatives.**
  - (a) Keep both: the ingress orders for itself, the cert manager serves
    everything else. Two ACME clients, two account stores, two sets of
    backoff and renewal rules, two docs saying different things about the
    same CA.
  - (b) The cert manager owns HTTP-01 entirely, through a jobspec key
    (cert-manager's ingress-shim) or by running its responder on 80; the
    ingress only serves bundles. The cert manager has no port 80 of its
    own, and a jobspec key that requests a certificate is blocked on
    extension values carrying authority (cert-manager calls entry 19).
  - (c) Make the cert manager an installed app and keep the ingress's
    client built in.
- **Trade-off.** The ingress now depends on a second bundled app: with the
  cert manager disabled, nothing is issued (the page says the Issuer is not
  ready). A Certificate per host is a visible object in `legion-system`
  that operators may wonder about; the labels (`ingress/managed`,
  `ingress/host`) say whose it is. The first issuance has one more hop
  (ingress → object → cert manager → CA), and a new certificate reaches
  every replica on the next 10-second KV sync, as before.
- **Why (simplicity, extensibility).** One ACME implementation, one place
  for renewal, backoff, rate limits, and account keys, and the ingress's
  surface shrinks to `certificates`, `issuer`, and `acme { server, email }`.
  The jobspec stays one line: a route still needs no certificate config.
  And the cert manager's Issuers become the ingress's for free: a DNS-01
  Issuer gives wildcard hosts, a `ca` Issuer gives internal names, EAB
  gives ZeroSSL, none of which the in-app client did. HTTP-01 stays where
  port 80 is (the ingress), answered statelessly from the Issuer's public
  thumbprint with the cert manager's own `http01::answer`, so no challenge
  state is shared. (a) is the duplication this removes; (b) has no port to
  answer on; (c) loses "incredibly easy" for every other certificate.

## 2. The app binds 80 and 443 itself, with an ambient `CAP_NET_BIND_SERVICE`, not behind the datapath's root listener

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

## 3. Routes live on the service as an app jobspec key, `service.ingress`, not as a route object or tags

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

## 4. The cert manager's `via = "objects"` KV rule names `legion/apps/*`, so it can write into an app's private prefix for that app's own Certificates

```nickel
{ resources = ["kv"], actions = ["read", "write"], paths = ["*", "legion/apps/*"], via = "objects" },
```

The end-to-end test found that `*` never reaches the reserved `legion/`
keys, so acting for the ingress's Certificate the cert manager was refused
writing `legion/apps/ingress/issued/<name>`:

```text
KvUnreadable: reading kv legion/apps/ingress/issued/shop-example-com-951623a26f: permission denied: kv:read is outside what certsx for Certificate legion-system/shop-example-com-951623a26f holds
```

- **Alternatives.**
  - (a) Name only the ingress's prefix (`legion/apps/ingress/issued/*`) in
    the cert manager's manifest.
  - (b) Keep the ingress's certificates outside the reserved prefix, under
    a tenant-reachable key such as `ingress/certs/`.
- **Trade-off.** The cert manager's grant is wider on paper: it may act on
  any app's prefix. The for-object authority still bounds every write to
  keys the Certificate's writer was checked for at apply time (the kind's
  `requires`), and only an app holds its own prefix, so in practice only
  the ingress's own Certificates can send it into `legion/apps/ingress/`.
- **Why (security, extensibility).** (b) would put TLS private keys where
  any `paths = ["*"]` reader can take them. (a) hard-codes one app into the
  cert manager's manifest; the general rule lets any app keep a cert
  manager certificate private the same way, with no change to the cert
  manager.

## 5. The ingress is seeded disabled; `legion app enable ingress` turns it on

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

## 6. Automatic HTTPS is on by default, and the app agrees to the CA's terms on the operator's behalf

With the app enabled, every routed public hostname gets a Let's Encrypt
certificate with no configuration, and HTTP redirects to HTTPS once one is
served. `certificates = false` turns it off (`acme.enabled = false` before
entry 1). That suits a cluster behind a TLS-terminating load balancer.

- **Alternatives.** Certificates off until the operator sets `acme.email`
  and an `agreeToTerms = true`, the cert-manager and Traefik shape; or no
  default Issuer at all, so nothing is issued until someone writes one.
- **Trade-off.** Enabling the app is agreeing to the CA's subscriber
  agreement, which the operator may not have read: the default `ingress`
  Issuer registers an account. A host whose DNS does not point at the
  ingress spends failed validations, bounded by the cert manager's backoff
  (5 minutes, doubling to 6 hours), which stays inside Let's Encrypt's
  limit of 5 failures per host per hour.
- **Why (simplicity).** Caddy made this the expectation, and it is what
  "incredibly easy" means for an ingress: a route is HTTPS. The public doc
  states the agreement in its HTTPS section. Names no public CA can issue
  for (`localhost`, `.internal`, `.test`, IP literals, single labels) are
  never asked for, so a dev cluster does not hammer the CA. The default
  Issuer serves only `legion-system`, so the agreement does not extend to
  tenants writing their own Certificates against it.

## 7. New Certificates wait until an HTTP-01 Issuer has registered its account

```text
waiting for the Issuer to register its ACME account
```

- **Alternatives.** Apply Certificates as soon as routes exist, and let the
  cert manager's retry cover a failed first challenge.
- **Trade-off.** On a fresh cluster the first certificate waits for the
  account registration (a few seconds) plus the ingress's 5-second
  thumbprint poll. With several replicas, one that has not read the
  thumbprint yet can still answer `503` for those seconds, and the cert
  manager retries after its backoff.
- **Why (ease).** The ingress answers HTTP-01 from the thumbprint. Without
  the gate, the first order on a new cluster races the account
  registration, and a lost race costs the cert manager's 5-minute backoff:
  exactly the first-run experience "incredibly easy" cannot have. The end
  to end test asserts one order and no failure.

## 8. Where to point DNS comes from the ingress's own catalog service, and the app checks each route's DNS from its node

```console
$ legion service resolve legion-system/ingress
```

The ingress's job declares `service = { name = "ingress", port = "http" }`;
the app resolves it back for its **Point DNS here** block (public addresses
first), and resolves every exact routed host for a DNS column: `points
here`, `points to <ips>`, or `no record`.

- **Alternatives.**
  - (a) A new CLI verb (`legion ingress addresses`) or a field on `legion
    app status`.
  - (b) Show only the node's cluster address (`LEGION_IP_HTTP`), as the
    first build did.
  - (c) Manage DNS records (external-dns style) through provider APIs.
- **Trade-off.** Listing `legion-system` from the CLI needs a rule that
  names it; tenants see the addresses on the page instead. The DNS check
  sees DNS as the ingress's node does, so split-horizon DNS or a CDN in
  front reads `points to` while the route works.
- **Why (ease, simplicity).** The catalog already carries each node's
  `publicAddress` (`LEGION_WORKER_PUBLIC_ADDRESS`), so declaring a service
  answers the question with an existing command, a JSON field scripts and
  agents can use, and no new RPC. (a) adds surface for one question. (b)
  showed the private address on every NAT'd cloud VM, the common case.
  (c) is a product of its own, and the cert manager already holds the
  DNS-provider credentials such a feature would want.

## 9. Endpoints are polled every 2 seconds, not watched

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

## 10. A per-request HTTP proxy, not the gateway's read-one-head-and-splice

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

## 11. Only a request that never reached a service is retried

A dial that fails, or a pooled connection that the service closed before the
request went out (hyper hands the request back), moves the request to
another endpoint, up to three. Nothing else is retried: not a `5xx`, not a
connection reset mid-response, and not an idempotent `GET` that failed after
it was sent.

- **Alternatives.** Retry idempotent methods on any failure and on `502`,
  `503`, or `504` from the service, under a budget. The datapath's HTTP mode
  does this for a task's upstreams (`docs/network-handlers.md`, "HTTP
  mode").
- **Trade-off.** A `GET` that a dying instance accepted and then dropped
  fails to the client with `502` instead of being re-sent. A service that
  answers `503` while it drains passes that `503` on.
- **Why (simplicity, safety).** An unsent request is the only case where a
  retry cannot run a handler twice, whatever the method. That covers what
  actually happens during deploys: a replaced instance refuses connections,
  and its pooled connections close. Retrying sent requests needs bodies
  buffered for replay, which costs memory on every request, and a retry
  budget, which is a policy surface of its own. Passive ejection keeps a dead
  endpoint from taking new requests in the meantime.

## 12. Backends get HTTP/1.1 only

- **Alternatives.** Speak h2c to backends that declare
  `meta.protocol = "h2c"` or `"grpc"`.
- **Trade-off.** gRPC services cannot be routed in this release, and a
  backend that would prefer HTTP/2 gets one request per connection at a
  time, pooled.
- **Why (simplicity).** HTTP/1.1 is what every web framework serves by
  default, and the pool and upgrade paths are simpler with one protocol.
  This is in Phase 3.

## 13. The first claim on a host and path wins; a conflicting submit is refused

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

## 14. The key is called `ingress`, beside the unrelated `network.ingress` chain

- **Alternatives.** `expose`, `public`, `route`, `http`, `domain`.
- **Trade-off.** One jobspec now holds `network.ingress`, the datapath
  policy on who may connect in, and `service.ingress`, the public route to
  the service. They mean different things.
- **Why (simplicity).** An app's key defaults to its name, so `ingress`
  lines up with `legion app … ingress`, `apps.ingress`, the docs page, and
  what every operator calls this. The two sit on different blocks, and the
  docs cross-reference them.

## 15. `host` takes a string or a list

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

## 16. One certificate per hostname

- **Alternatives.** One certificate per route, carrying all of its hosts
  as SANs.
- **Trade-off.** More orders: `example.com` and `www.example.com` are two.
  Let's Encrypt allows 50 per registered domain per week, which is ample
  for this.
- **Why (simplicity, robustness).** A host that fails validation, because
  its DNS is not pointed yet, does not block the others. Renewals and
  removals are per name. Nothing has to be re-ordered when a route gains or
  loses a host.

## 17. Routes to `mesh = true` services are reported, not served

- **Alternatives.** Give the app a node certificate so it can originate
  mesh mTLS with a PROXY v2 TLV, as the datapath does.
- **Trade-off.** A mesh service needs a second, non-mesh service on another
  port to be routed.
- **Why (security).** The node certificate is the node's identity. Handing
  it to an app process would let that app speak as the node to every mesh
  service. The view and the route's problems name the cause.

## 18. The ingress's own errors are the JSON envelope, not HTML pages

`404 no_route`, `503 no_endpoint`, `502`, and `504` come back as
`{ "ok": false, "error": { code, message, hint, retryable } }` with a
`Legion-Ingress-Error` header, like the gateway's.

- **Alternatives.** HTML error pages for browsers, chosen by `Accept`, the
  way nginx and Caddy answer.
- **Trade-off.** A person who opens a misrouted URL in a browser sees JSON.
- **Why (simplicity, agent-friendliness).** One shape, which scripts, load
  balancers, and agents can read without guessing, and which names the code
  and the next step. Every Legion surface already speaks it. A service that
  wants its own error pages serves them itself, behind a route.

## 19. A small RFC 8555 client in the tree instead of `instant-acme`

> **Superseded by entry 1.** The in-app ACME client was removed before
> merge; certificates come from the cert manager. Kept as history.

`src/apps/ingress/acme.rs` implements the five ACME exchanges Legion needs,
on crates already in the build: `ring` ES256 JWS, the RFC 7638 thumbprint,
`rcgen` CSRs, and async `reqwest`. Those exchanges are new account, new
order, the HTTP-01 challenge, finalize, and download.

- **Alternatives.** `instant-acme` 0.8 with its `ring` feature and its own
  HTTP client switched off. It is well maintained and has seen far more
  CAs.
- **Trade-off.** Legion owns the protocol code. Edge cases a library has
  already met are ours to meet: a CA's quirky `Retry-After`, an
  `orderNotReady` race, ARI. CI runs the whole issuance against an
  in-process CA written for the test
  (`ingress::orders_a_certificate_from_an_acme_ca_test`). That CA verifies
  every JWS, nonce, and URL, refuses one nonce, fetches the HTTP-01 answer
  from the ingress, and signs the CSR. But it is our reading of RFC 8555
  checking our reading of RFC 8555. The test against Pebble, Let's
  Encrypt's own test CA, runs only where a Pebble binary is present, which
  neither CI nor the session that built this had.
- **Why (performance of the build, simplicity of the tree).** The library
  would pull a second `rcgen` and a second `x509-parser` major unless its
  features are trimmed. With them trimmed, it still needs a hand-written
  HTTP client adapter, which is a third of the work of the client itself.
  The tree's pattern is to avoid a dependency when what it does is small
  and specified. Here it is about 400 lines, and RFC 8555 is stable.

## 20. One issuer per cluster, chosen by a KV lock; every replica serves from KV

> **Partly superseded by entry 1.** The challenge board and the ACME
> account are gone. The KV lock survives as the writer of the ingress's
> Certificates (`legion/apps/ingress/certificates/lock`, 90 s lease); every
> replica still serves from KV. Kept as history.

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
