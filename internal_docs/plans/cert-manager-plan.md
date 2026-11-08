# Plan: A Built-In Cert Manager (`certs` App)

> **Judgement calls:** [`calls/cert-manager-plan_calls.md`](../calls/cert-manager-plan_calls.md)

## Status (2026-11-07): SHIPPED (Phase 1)

Phase 1 shipped: the bundled `certs` app with `certs/Issuer` and
`certs/Certificate` objects, three issuer types (`selfSigned`, `ca`,
`acme`), ACME DNS-01 (Cloudflare and a webhook) and HTTP-01 (a stateless
responder, `legion cert http01`), renewal, status, a sidebar page and an
overview card, and `legion cert renew`. Public docs:
[`docs/cert-manager.md`](../../docs/cert-manager.md).

Deliberately deferred (each a TODO.md item): a job panel showing the
certificates a job serves; RSA keys; more DNS providers (Route 53, Google
Cloud DNS, RFC 2136); TLS-ALPN-01 and HTTP-01 answered by the ingress
datapath itself; a jobspec extension that creates certificates from a port
(cert-manager's ingress-shim); ACME Renewal Information (ARI); automatic CA
rotation; revocation; per-name issuer policy (cert-manager's
approver-policy).

## Problem statement

Getting a TLS certificate onto a Legion workload is three separate problems,
and Legion solves only the last one.

1. **Issuing.** Nothing in Legion talks to Let's Encrypt or signs a
   certificate for a workload. The only CA is the cluster CA, which is for
   Legion's own processes and is not something a workload can ask for a
   certificate from.
2. **Renewing.** A public certificate lasts 90 days today and is heading to
   45. Today a user copies `configs/jobs/cert-renewer.ncl`: a scheduled batch
   job running `lego` with a shell script per domain, a Cloudflare token, and
   an `openssl` check. It is one job per domain, it is shell, it reports
   nothing beyond its logs, and nothing tells you a renewal is failing until
   the certificate expires.
3. **Delivering.** Solved: `ports[].tls.cert = { kv }` serves a KV-held bundle
   from the node datapath and swaps it live, and `files` with
   `onChange = "update"` rewrites a file under a running task and signals it
   (kv-cert-delivery-plan, SHIPPED).

Internal TLS is worse: a service that wants TLS between its own components
(Postgres with `ssl = on`, a gRPC service with mTLS) has to run `openssl` by
hand, keep a CA key somewhere, and remember to re-sign before expiry.

The TODO list has carried "Cert manager built in, with public-CA routes,
usable by workloads and not just the control plane" since the apps substrate
landed; the app-objects plan names cert-manager as a motivating app.

## Prior art

- **Kubernetes cert-manager** ([docs](https://cert-manager.io/docs/)). The
  model everyone knows: `Issuer`/`ClusterIssuer` (ACME, CA, SelfSigned,
  Vault, Venafi), `Certificate` (dnsNames, secretName, duration,
  renewBefore, privateKey, usages), and internal `CertificateRequest`,
  `Order`, `Challenge` resources. It writes a `kubernetes.io/tls` Secret
  (`tls.crt`, `tls.key`, `ca.crt`) and renews at two thirds of the lifetime
  by default. HTTP-01 is solved by creating a solver pod, service, and
  ingress route; DNS-01 by a dozen built-in providers plus webhooks. The
  ingress-shim creates Certificates from Ingress annotations. `cmctl
  renew` forces a renewal. Failed issuance backs off exponentially from an
  hour. **We take:** the two object kinds and their field names, the
  renew-at-two-thirds default, status that says when the next renewal is,
  backoff that respects CA rate limits, and the three-part output (chain,
  key, CA). **We reject:** the four internal resources (an app's own memory
  and status cover them), Issuer *and* ClusterIssuer (one cluster-scoped
  Issuer with a `namespaces` list), and the SelfSigned-issuer-signs-a-CA-
  Certificate-referenced-by-a-CA-issuer dance (a `ca` issuer generates its
  own root).
- **trust-manager** distributes CA bundles. A `ca` issuer here publishes its
  certificate (no key) to a KV key of its own, which is the same job in one
  field.
- **Caddy and Traefik** issue certificates automatically for the names they
  route, because they own ports 80 and 443. Legion's node datapath owns a
  port's 443 when the port has `tls`, but there is no cluster edge
  (TODO.md, Networking), so the datapath cannot know which names reach it.
  We take Caddy's on-by-default posture (the app is bundled and seeded
  enabled) and its HTTP-01 answer, not its name discovery.
- **acme.sh stateless mode** answers every
  `/.well-known/acme-challenge/<token>` with `<token>.<thumbprint>`. The
  key authorization is the token plus the *public* account key's
  thumbprint, so a responder needs no per-challenge state and no secret.
  This is what makes HTTP-01 workable without an edge.
- **Nomad** has no certificate manager; users run Vault's PKI with
  `template` blocks. **Consul** issues mesh certificates only.
- **lego's `httpreq` DNS provider** posts `{ fqdn, value }` to
  `/present` and `/cleanup`. Our webhook uses that shape, so existing
  bridges (acme-dns proxies, internal DNS APIs) work unchanged.

## Jobs to be done

**App developer / job author**
- When I put a website on the cluster, I want a real HTTPS certificate for
  my domain with one short file, so I never think about certificates again.
- When my database or internal service needs TLS, I want a certificate from
  a private CA that renews on its own, so I am not running `openssl` by hand
  and finding out a year later that it expired.
- When my software needs the certificate and the key in separate files, I
  want them as separate files that update when the certificate renews, so I
  do not have to repackage anything.
- When a certificate cannot be issued, I want the reason in plain words
  where I already look, so I can fix the DNS record or the token.

**Cluster operator**
- When I set up the cluster, I want to configure Let's Encrypt once for
  everybody, so every team gets certificates without their own tokens.
- When a renewal is failing, I want to see it weeks before expiry, in the
  UI and on the CLI, so it never becomes an outage.
- When I need a certificate re-issued now (a key leaked, I changed DNS), I
  want one command, so I do not have to delete anything.

**Security / platform admin**
- When teams share a private CA, I want to say which namespaces may use it,
  so one team cannot mint certificates another team's clients trust.
- When someone asks for a certificate, I want them to need write access to
  the place it is stored, so a certificate request cannot overwrite secrets
  they could not write themselves.
- When the cert manager holds DNS and CA credentials, I want it to read
  them only on behalf of the issuer that names them, so the app's own grant
  stays small.

**Autonomous agent**
- When I deploy a service, I want to request its certificate with the same
  object API I use for everything else, wait until it is ready, and read
  why when it is not, so I can self-correct without a human.

## User scenarios

1. **A public website.** Ana runs one node with a public IP and a domain.
   She applies an Issuer for Let's Encrypt with Cloudflare DNS-01 (the token
   already in KV), applies a Certificate naming `shop.example.com` and
   `certs/shop`, and adds `tls = { cert = { kv = "certs/shop" } }` to her
   port 443. `legion object get certificate shop --wait` turns `Ready` a
   minute later; `curl https://shop.example.com` works. Sixty days later the
   certificate renews and the node swaps it without a restart.
2. **Postgres with TLS.** Ben applies `ca = { kv = "certs/ca/internal",
   trust = "certs/ca/internal.pem" }`; the app generates the CA. His
   Certificate names `db.internal` and writes `parts = { cert, key }`; the
   Postgres task renders both as files with `onChange = "update"` and
   `changeMode = "signal"`. His API job trusts the CA through
   `connect.upstreams[].ca = { kv = "certs/ca/internal.pem" }`.
3. **A failing renewal.** The Cloudflare token was rotated and the old one
   revoked. The Certificate reads `NotReady: IssueFailed: cloudflare: 403
   ...`, its status shows `renewalTime` passed and the next attempt in 20
   minutes, the overview card says `1 failing`, and the certificate in KV is
   still the old, valid one. Carla puts the new token in KV and runs
   `legion cert renew shop`.

## Goals and non-goals

Goals:
- Cover cert-manager's core: Issuer + Certificate objects; ACME (Let's
  Encrypt and any RFC 8555 CA, with EAB), CA, and self-signed issuers;
  DNS-01 and HTTP-01; automatic renewal; status and failure reporting;
  forced renewal.
- Zero setup for the app itself: bundled, seeded, enabled.
- Output in the format the datapath and live files already consume, so
  "integrate into an app" is one existing line.
- Least privilege: every key the app reads or writes for an object, it
  reads or writes *for that object*, bounded by what the object's writer
  holds.

Non-goals:
- A cluster edge or name discovery (Caddy's automatic HTTPS).
- A second CA for Legion's own processes (the cluster CA stays as is).
- Every DNS provider. Cloudflare plus a webhook covers most, and the
  webhook bridges the rest.
- RSA keys: `ring` cannot generate them.
- Revocation, OCSP, CRLs.

## Background: what exists today

- **Apps** (`src/apps/mod.rs`): `BUNDLED` lists the bundled apps (today the
  autoscaler); a bundled app is a Nickel manifest in `src/apps/bundled/`
  plus a body `fn(AppContext) -> AppFuture`, run by `legion:self app run
  <name>` on the managers' system workers, seeded by the first Control
  leader with its permissions granted by `legion:bootstrap`.
- **Object kinds** (`src/apps/objects.rs`, docs/app-objects.md): the
  manager validates a kind's schema, CEL `rules`, and `requires` on every
  apply, records the writer's **authority**; the app watches with
  `WatchObjects`, reports with `UpdateObjectStatus`, and calls Legion
  **for an object** with `legion-for-object`
  (`ManagerConnection::client_for_object`), holding only that object's
  authority intersected with its grant, `via = "objects"` rules included.
  `Reconciler` gives a level-triggered loop with backoff and
  status-on-change.
- **KV** (`PutKv` with `expected_version = 0` creates only if absent).
  `GetKv` returns the value and version.
- **Delivery**: `validate_bundle` in `src/worker/netd/tls.rs` is the check
  the manager runs before handing a KV bundle to the datapath: a chain, a
  private key, and a key matching the leaf.
- **Crypto**: `rcgen` 0.13 (with `x509-parser`), `ring` 0.17,
  `x509-parser` 0.16, `rustls`, `reqwest` (rustls, webpki roots), `base64`,
  `sha2` are already dependencies. Nothing new is needed for RFC 8555.
- **`legion:self`** resolves to the worker's own binary for any exec task
  (`src/task_plugins/exec.rs:979`).

## Proposed design

### 1. The `certs` app

A bundled app, `certs`, placed on the managers with one replica,
like the autoscaler. Its grant:

```nickel
permissions = [
  # Its own state: one ACME account key per Issuer.
  { resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/certs/*"] },
  # Certificates, CA material, DNS tokens: only for one of its objects,
  # and only where that object's writer could read or write.
  { resources = ["kv"], actions = ["read", "write"], paths = ["*"], via = "objects" },
]
```

It holds nothing else. `via = "objects"` means the app cannot touch a
single user key on its own credential.

### 2. `certs/Issuer` (cluster-scoped)

Exactly one of three types:

```nickel
# Let's Encrypt, DNS-01 through Cloudflare (wildcards work).
{ kind = "certs/Issuer", name = "letsencrypt",
  spec = { acme = {
    email = "ops@example.com",                 # optional
    # server = "letsencrypt" (default) | "letsencrypt-staging" | any directory URL
    dns01 = { cloudflare = { apiToken = { kv = "secrets/cloudflare-dns" } } },
  } } }

# A private CA, generated on first use and kept in KV.
{ kind = "certs/Issuer", name = "internal",
  spec = { ca = { kv = "certs/ca/internal", trust = "certs/ca/internal.pem" } } }

# Self-signed: each certificate signs itself.
{ kind = "certs/Issuer", name = "self", spec = { selfSigned = {} } }
```

| Field | Meaning |
| --- | --- |
| `namespaces` | Namespaces (globs) whose Certificates may use it. Default every namespace. |
| `acme.server` | `letsencrypt` (default), `letsencrypt-staging`, or a directory URL. |
| `acme.email` | Contact for the account. Optional. |
| `acme.eab` | `{ kid, hmacKey = { kv } }`: external account binding (ZeroSSL, Google). |
| `acme.dns01` | `{ cloudflare = { apiToken = { kv } } }` or `{ webhook = { url, token? = { kv } } }`, plus `propagationDelay` (default `30s`). |
| `acme.http01` | `{}`: answer HTTP-01 through `legion cert http01` responders. |
| `ca.kv` | The CA's bundle (certificate, then PKCS#8 key). Generated (ECDSA P-256, 10 years) when the key does not exist. |
| `ca.trust` | Where to publish the CA certificate alone, for clients to trust. |

`requires` makes the writer hold `kv` read (and write, for `ca.kv`) on
every key the Issuer names; the Issuer's writer must already be a
cluster-wide `object` writer (a cluster kind), so Issuers are an operator's
setup. Status fields: `type`, `server`, `accountUri`, `thumbprint` (for
HTTP-01 responders), `caSubject`, `caNotAfter`, `generated`.

### 3. `certs/Certificate` (namespaced)

```nickel
{ kind = "certs/Certificate", name = "shop", namespace = "default",
  spec = {
    issuer = "letsencrypt",
    dnsNames = ["shop.example.com", "www.shop.example.com"],
    kv = "certs/shop",                      # chain, then key: what ports[].tls.cert reads
    # parts = { cert = "certs/shop.crt", key = "certs/shop.key", ca = "certs/shop-ca.crt" },
    # ipAddresses = [], commonName, duration = "2160h", renewBefore,
    # keyAlgorithm = "ecdsa-p256" | "ecdsa-p384", usages = ["server", "client"]
  } }
```

- `requires`: `kv` read and write on `spec.kv`, write on each `parts` key.
  The app writes them **acting for the Certificate**.
- Issuance writes the bundle in one `PutKv` (atomic for every consumer of
  `spec.kv`), then the parts, key first.
- A new key on every issuance.
- **When it issues**: no valid bundle in `spec.kv`; the bundle's names or
  key algorithm differ from the spec; the issuance-relevant spec changed
  (a hash in status); the Issuer changed; it is past its renewal time
  (`notAfter - renewBefore`, default two thirds through its lifetime); or a
  renewal was requested (`legion cert renew`, below).
- **Backoff**: a failed issuance waits 5 minutes, doubling to 6 hours,
  tracked per Certificate and seeded from status after a restart, so a
  broken token never hits Let's Encrypt's "5 failures per hour" limit. The
  previous certificate stays in KV and keeps serving.
- Status: `Ready` with reason `Issued`, `RenewalFailing` (still `Ready`:
  the certificate in KV is valid), `IssueFailed`, `IssuerNotFound`,
  `IssuerNotReady`, `KvUnreadable`, `PartsNotWritten`; `Accepted=false` for
  `IssuerNotAllowed` and spec problems (a wildcard on an issuer without
  DNS-01). Fields: `notBefore`, `notAfter`, `renewalTime`, `serial`,
  `issuer`, `kv`, `names`, `revision`, `failedAttempts`, `lastFailure`,
  `lastFailureTime`, `nextAttemptTime`, and the bookkeeping that survives a
  restart (`specHash`, `issuerIdentity`, `renewRequest`).

```console
$ legion object list certificates -A
NAMESPACE  NAME  STATUS  AGE  ISSUER       KV          EXPIRES
default    shop  Ready   3d   letsencrypt  certs/shop  2027-02-04T10:12:00Z
```

### 4. ACME

An RFC 8555 client in `src/apps/certs/acme.rs`: ES256 JWS from `ring`,
nonce handling with `badNonce` retry, account creation (with EAB), orders,
authorizations, `dns-01` and `http-01`, finalize with an `rcgen` CSR, and
the PEM chain. One account key per Issuer, in the app's own
`legion/apps/certs/acme/<issuer uid>.key`. DNS-01 is preferred when the
Issuer has it (it works for every name); HTTP-01 otherwise.

**HTTP-01 without a cluster edge.** The app cannot know which node a domain
points at, so it does not try. The key authorization is
`<token>.<account thumbprint>`, and the thumbprint is public, so any
process that answers port 80 for the domain can answer every challenge
statelessly:

```console
$ legion cert http01 --issuer letsencrypt --listen 0.0.0.0:80
```

reads the Issuer's thumbprint (it needs `object:read` on `certs/Issuer`),
answers `/.well-known/acme-challenge/<token>`, and redirects everything else
to HTTPS. `configs/certs/http01-responder.ncl` runs it on every node of a
pool. A user's own nginx can do the same with one `location` and the
thumbprint from `legion object get issuer letsencrypt`.

### 5. Forced renewal

```console
$ legion cert renew shop -n default
certs/Certificate default/shop: renewal requested; the certs app re-issues it within seconds
```

sets the label `certs/renew` to the current time (an ordinary apply, so it
needs what writing the Certificate needs); the app re-issues once per new
value and records it in status.

### 6. UI

A **Certificates** page in the sidebar (every certificate the viewer may
read, its names, issuer, state, and expiry, and every issuer) and a card on
the cluster overview (ready, renewing within 14 days, failing), both
filtered by the viewer's `object:read` namespaces.

## Proto, struct, config, and persistence changes

- No proto, storage, or jobspec changes. Everything is an app: a manifest,
  a body, two kinds, a CLI group.
- `src/apps/bundled/certs.ncl`; `BUNDLED` gains `certs`.
- `legion cert` (`renew`, `http01`); `cert` joins `RESERVED_KIND_NOUNS`.
- The app's config is empty (`{}`): everything a user sets lives on the
  objects, so the config schema accepts no fields.

## Implementation phases

Phase 1 (this change) ships the whole of §1-§6; each piece is useless
without the app, so it is one phase:

1. Specs, private issuance (CA, self-signed), bundle parsing, renewal math.
2. ACME client, DNS-01 providers, HTTP-01 responder, a fake ACME server
   for tests.
3. The bundled app: manifest, reconcilers, views.
4. `legion cert`.
5. Docs.

Phase 2 (deferred, each its own TODO item): the job panel, more DNS
providers, the ingress-shim extension, ARI, datapath-answered challenges.

## Testing plan

- **Unit**: spec parsing and defaults; renewal time and backoff math;
  CA generation, BYO CA, leaf signing verified by `webpki` for each usage;
  self-signed; the bundle passes `validate_bundle`; names and key-algorithm
  drift detection; JWS signatures verified against the JWK; the
  thumbprint against RFC 7638's example; EAB; the responder's answers and
  redirects; Cloudflare's zone walk against a fake API; the webhook body.
- **ACME end to end, in process**: a fake RFC 8555 server
  (`certs::test_support`) that checks every JWS, dials the responder for
  HTTP-01 and reads a fake DNS for DNS-01, and issues from its own CA.
- **Manifest**: the bundled manifest evaluates, its kinds install, its
  examples pass, its grant is exactly the two rules above.
- **Integration** (`tests/integration/certs.rs`): an in-process manager;
  apply a CA Issuer and a Certificate, run the engine, and see the bundle in
  KV accepted by `validate_bundle`, the CA generated and published, status
  `Ready` with `notAfter`; renewal through a forced clock; `legion cert
  renew`'s label re-issues; a namespace the Issuer does not allow is
  rejected; ACME end to end against the fake server through a webhook
  DNS-01; with ACLs on, applying a Certificate without `kv:write` on its key
  is refused.

## Rollout and backward compatibility

- New clusters and existing ones get the app at the next Control leader
  pass, enabled, like any bundled app. It does nothing until someone
  applies an Issuer. `legion app disable certs` turns it off.
- `configs/jobs/cert-renewer.ncl` stays as the bring-your-own-ACME-client
  example, with a pointer to the app.
- No migration.

## Open questions

- Should `namespaces` also restrict DNS names per issuer? Lean: later, as
  an approver-policy-like `names` list, once someone shares a CA across
  teams that need it.

## Acceptance criteria

- `legion object apply` of a CA Issuer and a Certificate puts a bundle in KV
  that a port's `tls.cert = { kv }` serves, with no other step.
- The same against Let's Encrypt staging with Cloudflare DNS-01, verified by
  hand (CI uses the fake server).
- A Certificate past its renewal time is re-issued; a failing one backs off
  and keeps the old certificate.
- `legion cert renew` re-issues once.
